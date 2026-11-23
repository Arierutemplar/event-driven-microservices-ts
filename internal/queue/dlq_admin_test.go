package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
)

// poisonRepo falla de forma permanente con los eventos "poison*" hasta que se marca como arreglado.
func poisonRepo() (*fakeRepo, *atomic.Bool) {
	var fixed atomic.Bool
	return &fakeRepo{fail: func(batch []*domain.Event) error {
		if fixed.Load() {
			return nil
		}
		for _, e := range batch {
			if strings.HasPrefix(e.UserID, "poison") {
				return fmt.Errorf("esquema inválido: %w", domain.ErrPermanent)
			}
		}
		return nil
	}}, &fixed
}

func dlqAdmin(t *testing.T, c *Client) *DLQAdmin {
	t.Helper()
	if c.DLQ() == nil {
		t.Fatal("Client.DLQ() es nil")
	}
	return c.DLQ()
}

func waitDLQ(t *testing.T, c *Client, n uint64) {
	t.Helper()
	eventually(t, fmt.Sprintf("%d mensajes en la DLQ", n), func() bool {
		return streamMsgs(t, c, c.cfg.Stream+"_DLQ") == n
	})
}

func TestDLQListGetAndFilter(t *testing.T) {
	repo, _ := poisonRepo()
	c := startClient(t, repo, nil)
	a := dlqAdmin(t, c)
	ctx := context.Background()

	publish(t, c, "poison-1")
	publish(t, c, "poison-2")
	waitDLQ(t, c, 2)

	st, err := a.Stats(ctx)
	if err != nil || st.Messages != 2 {
		t.Fatalf("Stats = %+v, %v", st, err)
	}
	items, err := a.List(ctx, domain.DLQFilter{}, 0)
	if err != nil || len(items) != 2 {
		t.Fatalf("List = %d, %v", len(items), err)
	}
	if items[0].Reason != "permanent_error" || items[0].EventID != "poison-1" || items[0].TenantID != domain.DefaultTenant {
		t.Fatalf("metadatos inesperados: %+v", items[0])
	}
	if items[0].Body != "" || items[0].BodyPreview == "" {
		t.Fatalf("el listado debe traer preview y no cuerpo: %+v", items[0])
	}

	if got, _ := a.List(ctx, domain.DLQFilter{Reason: "decode_error"}, 0); len(got) != 0 {
		t.Fatalf("filtro por motivo debería excluir todo: %d", len(got))
	}
	if got, _ := a.List(ctx, domain.DLQFilter{AfterSeq: items[0].Seq}, 0); len(got) != 1 || got[0].EventID != "poison-2" {
		t.Fatalf("AfterSeq: %+v", got)
	}
	if got, _ := a.List(ctx, domain.DLQFilter{Seqs: []uint64{items[1].Seq, 9999}}, 0); len(got) != 1 {
		t.Fatalf("Seqs con inexistente: %d", len(got))
	}
	if got, _ := a.List(ctx, domain.DLQFilter{}, 1); len(got) != 1 {
		t.Fatalf("limit=1: %d", len(got))
	}

	d, err := a.Get(ctx, items[0].Seq)
	if err != nil || !strings.Contains(d.Body, "poison-1") {
		t.Fatalf("Get = %+v, %v", d, err)
	}
	if _, err := a.Get(ctx, 9999); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Get inexistente debería ser ErrNotFound, got %v", err)
	}
}

func TestRedriveDryRunChangesNothing(t *testing.T) {
	repo, _ := poisonRepo()
	c := startClient(t, repo, nil)
	a := dlqAdmin(t, c)

	publish(t, c, "poison-1")
	waitDLQ(t, c, 1)

	res, err := a.Redrive(context.Background(), domain.DLQFilter{}, 0, true)
	if err != nil || !res.DryRun || res.Matched != 1 || res.Republished != 0 {
		t.Fatalf("dry-run = %+v, %v", res, err)
	}
	if n := streamMsgs(t, c, c.cfg.Stream+"_DLQ"); n != 1 {
		t.Fatalf("la DLQ debe seguir con 1 mensaje, tiene %d", n)
	}
}

func TestRedrivePersistsAfterFixAndEmptiesDLQ(t *testing.T) {
	repo, fixed := poisonRepo()
	c := startClient(t, repo, nil)
	a := dlqAdmin(t, c)

	publish(t, c, "poison-1")
	publish(t, c, "ok-1")
	waitDLQ(t, c, 1)
	eventually(t, "ok-1 persistido", func() bool { return repo.savedIDs()["ok-1"] == 1 })

	fixed.Store(true)
	// Redrive inmediato: dentro de la ventana de dedup del broker. No debe descartarse.
	res, err := a.Redrive(context.Background(), domain.DLQFilter{Reason: "permanent_error"}, 0, false)
	if err != nil || res.Republished != 1 || res.Duplicates != 0 || res.Failed != 0 || res.DeleteFailed != 0 {
		t.Fatalf("Redrive = %+v, %v", res, err)
	}
	eventually(t, "poison-1 persistido tras el re-drive", func() bool { return repo.savedIDs()["poison-1"] == 1 })
	waitDLQ(t, c, 0)
	if got := repo.savedIDs()["poison-1"]; got != 1 {
		t.Fatalf("poison-1 persistido %d veces", got)
	}
}

func TestRedriveRewritesHeaders(t *testing.T) {
	repo, _ := poisonRepo()
	c := startClient(t, repo, func(cfg *Config) { cfg.MaxDeliver = 3 })
	a := dlqAdmin(t, c)

	publish(t, c, "poison-1")
	waitDLQ(t, c, 1)
	orig := firstDLQ(t, c)
	origID := orig.Header.Get("Nats-Msg-Id")

	if _, err := a.Redrive(context.Background(), domain.DLQFilter{}, 0, false); err != nil {
		t.Fatal(err)
	}
	// Al seguir fallando vuelve a la DLQ con el contador incrementado y sin duplicar metadatos
	eventually(t, "mensaje de vuelta en la DLQ", func() bool {
		l, _ := a.List(context.Background(), domain.DLQFilter{AfterSeq: 1}, 0)
		return len(l) == 1
	})
	l, _ := a.List(context.Background(), domain.DLQFilter{AfterSeq: 1}, 0)
	if l[0].RedriveCount != 1 {
		t.Fatalf("RedriveCount = %d, want 1", l[0].RedriveCount)
	}
	d, err := a.Get(context.Background(), l[0].Seq)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(d.MsgID, origID) || !strings.Contains(d.MsgID, "#dlq1") {
		t.Fatalf("MsgID = %q, original %q", d.MsgID, origID)
	}
	if d.OriginalSeq == 0 {
		t.Fatal("el nuevo fallo debe traer X-DLQ-Stream-Seq propio")
	}
	// La entrada antigua ya no existe
	if _, err := a.Get(context.Background(), 1); err == nil {
		t.Fatal("la entrada original debía borrarse de la DLQ")
	}

	// Segundo re-drive: el id no crece (sin #dlq1#dlq2)
	if _, err := a.Redrive(context.Background(), domain.DLQFilter{}, 0, false); err != nil {
		t.Fatal(err)
	}
	eventually(t, "segundo retorno a la DLQ", func() bool {
		l, _ := a.List(context.Background(), domain.DLQFilter{AfterSeq: l[0].Seq}, 0)
		return len(l) == 1 && l[0].RedriveCount == 2
	})
	l2, _ := a.List(context.Background(), domain.DLQFilter{AfterSeq: l[0].Seq}, 0)
	if strings.Count(l2[0].MsgID, "#dlq") != 1 {
		t.Fatalf("el MsgID acumula sufijos: %q", l2[0].MsgID)
	}
}

func TestRedriveLimitAndTruncated(t *testing.T) {
	repo, fixed := poisonRepo()
	c := startClient(t, repo, nil)
	a := dlqAdmin(t, c)

	for i := 0; i < 5; i++ {
		publish(t, c, fmt.Sprintf("poison-%d", i))
	}
	waitDLQ(t, c, 5)
	fixed.Store(true)

	res, err := a.Redrive(context.Background(), domain.DLQFilter{}, 2, false)
	if err != nil || res.Republished != 2 || !res.Truncated || res.Matched != 2 {
		t.Fatalf("Redrive limit = %+v, %v", res, err)
	}
	waitDLQ(t, c, 3)
	eventually(t, "2 persistidos", func() bool { return repo.savedCount() == 2 })
}

func TestRedriveSelectiveBySeq(t *testing.T) {
	repo, fixed := poisonRepo()
	c := startClient(t, repo, nil)
	a := dlqAdmin(t, c)

	publish(t, c, "poison-a")
	publish(t, c, "poison-b")
	waitDLQ(t, c, 2)
	fixed.Store(true)

	items, _ := a.List(context.Background(), domain.DLQFilter{}, 0)
	res, err := a.Redrive(context.Background(), domain.DLQFilter{Seqs: []uint64{items[1].Seq, 777}}, 0, false)
	if err != nil || res.Republished != 1 {
		t.Fatalf("Redrive = %+v, %v", res, err)
	}
	eventually(t, "solo poison-b persistido", func() bool { return repo.savedIDs()["poison-b"] == 1 })
	waitDLQ(t, c, 1)
	if repo.savedIDs()["poison-a"] != 0 {
		t.Fatal("poison-a no debía reinyectarse")
	}
}

func TestConcurrentRedrivesDoNotDuplicate(t *testing.T) {
	repo, fixed := poisonRepo()
	c := startClient(t, repo, nil)
	a := dlqAdmin(t, c)

	const n = 20
	for i := 0; i < n; i++ {
		publish(t, c, fmt.Sprintf("poison-%d", i))
	}
	waitDLQ(t, c, n)
	fixed.Store(true)

	var wg sync.WaitGroup
	var republished atomic.Int64
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := a.Redrive(context.Background(), domain.DLQFilter{}, 0, false)
			if err != nil {
				t.Errorf("Redrive: %v", err)
			}
			republished.Add(int64(res.Republished))
		}()
	}
	wg.Wait()

	if got := republished.Load(); got != n {
		t.Fatalf("reinyectados en total = %d, want %d (cada mensaje una sola vez)", got, n)
	}
	waitDLQ(t, c, 0)
	eventually(t, "todos persistidos", func() bool { return repo.savedCount() == n })
	for id, cnt := range repo.savedIDs() {
		if cnt != 1 {
			t.Fatalf("%s persistido %d veces", id, cnt)
		}
	}
}

func TestConnectAdminRequiresExistingStreams(t *testing.T) {
	c := startClient(t, &fakeRepo{}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a, err := ConnectAdmin(ctx, c.cfg)
	if err != nil {
		t.Fatalf("ConnectAdmin: %v", err)
	}
	defer a.Close()
	if st, err := a.Stats(ctx); err != nil || st.Stream != c.cfg.Stream+"_DLQ" {
		t.Fatalf("Stats = %+v, %v", st, err)
	}

	bad := c.cfg
	bad.Stream = "NOEXISTE" + c.cfg.Stream
	if _, err := ConnectAdmin(ctx, bad); err == nil {
		t.Fatal("ConnectAdmin debe fallar si el stream no existe (no debe crearlo)")
	}
}
