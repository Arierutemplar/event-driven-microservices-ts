// Command dlqctl es la herramienta de operaciones para la Dead Letter Queue de NATS JetStream:
// listar mensajes muertos, inspeccionarlos y reinyectarlos (re-drive) al subject principal.
//
//	dlqctl list    [--reason R] [--after SEQ] [--limit N] [--json]
//	dlqctl show    <seq>
//	dlqctl redrive (--seq 1,2,3 | --reason R | --all) [--limit N] [--dry-run] [--yes]
//
// Configuración (flag > variable de entorno > valor por defecto): NATS_URL, NATS_STREAM,
// NATS_SUBJECT, NATS_DLQ_SUBJECT.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/queue"
)

const usage = `uso: dlqctl <comando> [opciones]

comandos:
  list      lista los mensajes de la DLQ
  show      muestra un mensaje completo:  dlqctl show <seq>
  redrive   reinyecta mensajes en el subject principal

opciones comunes:
  --nats-url URL   (env NATS_URL, por defecto nats://localhost:4222)
  --stream NAME    (env NATS_STREAM, por defecto EVENTS)
  --subject S      (env NATS_SUBJECT, por defecto events.ingest)
  --dlq-subject S  (env NATS_DLQ_SUBJECT, por defecto analytics.dlq)

Usa "dlqctl <comando> -h" para ver las opciones de cada comando.`

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type common struct{ url, stream, subject, dlq string }

func (c *common) register(fs *flag.FlagSet) {
	fs.StringVar(&c.url, "nats-url", getenv("NATS_URL", "nats://localhost:4222"), "URL de NATS")
	fs.StringVar(&c.stream, "stream", getenv("NATS_STREAM", "EVENTS"), "stream principal")
	fs.StringVar(&c.subject, "subject", getenv("NATS_SUBJECT", "events.ingest"), "subject principal")
	fs.StringVar(&c.dlq, "dlq-subject", getenv("NATS_DLQ_SUBJECT", "analytics.dlq"), "subject de la DLQ")
}

func (c *common) connect(ctx context.Context) (*queue.DLQAdmin, error) {
	return queue.ConnectAdmin(ctx, queue.Config{URL: c.url, Stream: c.stream, Subject: c.subject, DLQSubject: c.dlq})
}

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprintln(os.Stderr, usage)
		if len(args) == 0 {
			return 2
		}
		return 0
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch args[0] {
	case "list":
		err = cmdList(ctx, args[1:])
	case "show":
		err = cmdShow(ctx, args[1:])
	case "redrive":
		err = cmdRedrive(ctx, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "comando desconocido %q\n\n%s\n", args[0], usage)
		return 2
	}
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	return 0
}

func cmdList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	var c common
	c.register(fs)
	reason := fs.String("reason", "", "filtrar por motivo (decode_error, permanent_error, max_deliveries_exceeded)")
	after := fs.Uint64("after", 0, "solo mensajes con secuencia posterior a SEQ")
	limit := fs.Int("limit", queue.DefaultListLimit, "máximo de mensajes a listar")
	asJSON := fs.Bool("json", false, "salida en JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	a, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer closeAdmin(a)

	stats, err := a.Stats(ctx)
	if err != nil {
		return err
	}
	items, err := a.List(ctx, domain.DLQFilter{AfterSeq: *after, Reason: *reason}, *limit)
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"stats": stats, "messages": items})
	}
	fmt.Printf("DLQ %s (%s): %d mensajes, %d bytes\n", stats.Stream, stats.Subject, stats.Messages, stats.Bytes)
	if len(items) == 0 {
		fmt.Println("sin mensajes que coincidan")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "SEQ\tMOTIVO\tTENANT\tEVENT_ID\tENTREGAS\tREDRIVES\tFECHA\tERROR")
	for _, d := range items {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%d\t%d\t%s\t%s\n", d.Seq, d.Reason, dash(d.TenantID), dash(d.EventID),
			d.Deliveries, d.RedriveCount, d.FailedAt.Local().Format("2006-01-02 15:04:05"), trunc(d.Error, 60))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if uint64(len(items)) < stats.Messages && *reason == "" && *after == 0 {
		fmt.Printf("(mostrando %d de %d; usa --limit)\n", len(items), stats.Messages)
	}
	return nil
}

func cmdShow(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("show", flag.ContinueOnError)
	var c common
	c.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("uso: dlqctl show <seq>")
	}
	seq, err := strconv.ParseUint(fs.Arg(0), 10, 64)
	if err != nil || seq == 0 {
		return fmt.Errorf("seq inválido: %q", fs.Arg(0))
	}
	a, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer closeAdmin(a)
	d, err := a.Get(ctx, seq)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(d)
}

func cmdRedrive(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("redrive", flag.ContinueOnError)
	var c common
	c.register(fs)
	seqs := fs.String("seq", "", "secuencias a reinyectar, separadas por comas")
	reason := fs.String("reason", "", "reinyectar solo los de este motivo")
	all := fs.Bool("all", false, "reinyectar toda la DLQ (hasta --limit)")
	limit := fs.Int("limit", queue.DefaultRedriveLimit, fmt.Sprintf("máximo de mensajes (tope %d)", queue.MaxRedriveLimit))
	dry := fs.Bool("dry-run", false, "solo muestra qué se reinyectaría")
	yes := fs.Bool("yes", false, "no pedir confirmación")
	if err := fs.Parse(args); err != nil {
		return err
	}
	f := domain.DLQFilter{Reason: *reason}
	for _, s := range strings.Split(*seqs, ",") {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil || n == 0 {
			return fmt.Errorf("--seq inválido: %q", s)
		}
		f.Seqs = append(f.Seqs, n)
	}
	if len(f.Seqs) == 0 && f.Reason == "" && !*all {
		return fmt.Errorf("indica qué reinyectar: --seq, --reason o --all")
	}
	if *limit < 1 {
		return fmt.Errorf("--limit debe ser >= 1")
	}

	a, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer closeAdmin(a)

	if !*dry {
		plan, err := a.Redrive(ctx, f, *limit, true)
		if err != nil {
			return err
		}
		if plan.Matched == 0 {
			fmt.Println("nada que reinyectar")
			return nil
		}
		if !*yes && !confirm(fmt.Sprintf("¿Reinyectar %d mensajes en %q? [s/N] ", plan.Matched, c.subject)) {
			fmt.Println("cancelado")
			return nil
		}
	}

	res, err := a.Redrive(ctx, f, *limit, *dry)
	printResult(res)
	if err != nil {
		return err
	}
	if res.Failed > 0 || res.DeleteFailed > 0 {
		return fmt.Errorf("re-drive incompleto: %s", res.FirstError)
	}
	return nil
}

func printResult(r domain.RedriveResult) {
	if r.DryRun {
		fmt.Printf("[dry-run] coinciden %d mensajes: seqs=%v\n", r.Matched, r.Seqs)
	} else {
		fmt.Printf("coinciden=%d reinyectados=%d duplicados=%d fallidos=%d sin_borrar=%d\n",
			r.Matched, r.Republished, r.Duplicates, r.Failed, r.DeleteFailed)
	}
	if r.Truncated {
		fmt.Println("hay más mensajes que coinciden; repite el comando o sube --limit")
	}
}

func confirm(prompt string) bool {
	fmt.Fprint(os.Stderr, prompt)
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && line == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "s", "si", "sí", "y", "yes":
		return true
	}
	return false
}

func closeAdmin(a *queue.DLQAdmin) {
	if err := a.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "aviso: cerrar conexión NATS:", err)
	}
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
