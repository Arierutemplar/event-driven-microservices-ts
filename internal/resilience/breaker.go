// Package resilience agrupa patrones de tolerancia a fallos.
package resilience

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/observability"
)

// ErrOpen indica que la operación se rechazó sin ejecutarse porque el circuito está abierto.
var ErrOpen = errors.New("resilience: circuit breaker abierto")

// errSlow marca internamente una llamada correcta pero demasiado lenta.
var errSlow = errors.New("resilience: latencia crítica")

// State del circuito. Los valores coinciden con los del gauge de Prometheus.
type State int

const (
	Closed State = iota
	HalfOpen
	Open
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case HalfOpen:
		return "half_open"
	default:
		return "open"
	}
}

const pollInterval = 200 * time.Millisecond

// Config parametriza el breaker.
type Config struct {
	Name                string
	ConsecutiveFailures uint32        // fallos seguidos que abren el circuito (defecto 5)
	OpenTimeout         time.Duration // tiempo abierto antes de pasar a semiabierto (defecto 15s)
	SlowThreshold       time.Duration // una llamada más lenta cuenta como fallo; 0 lo desactiva
	// IsFailure decide si un error debe contar contra el circuito. Por defecto, cualquier error.
	// Los errores causados por los datos (no por el servicio) no deberían contar.
	IsFailure func(error) bool
}

// Breaker envuelve un circuit breaker de gobreaker con métricas y detección de latencia.
type Breaker struct {
	name string
	slow time.Duration
	cb   *gobreaker.CircuitBreaker[struct{}]
}

// New crea el breaker, cerrado.
func New(cfg Config) *Breaker {
	if cfg.Name == "" {
		cfg.Name = "default"
	}
	if cfg.ConsecutiveFailures == 0 {
		cfg.ConsecutiveFailures = 5
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 15 * time.Second
	}
	isFailure := cfg.IsFailure
	if isFailure == nil {
		isFailure = func(err error) bool { return err != nil }
	}

	observability.BreakerInit(cfg.Name)
	return &Breaker{
		name: cfg.Name,
		slow: cfg.SlowThreshold,
		cb: gobreaker.NewCircuitBreaker[struct{}](gobreaker.Settings{
			Name:        cfg.Name,
			MaxRequests: 1, // una sola operación de prueba en semiabierto
			Timeout:     cfg.OpenTimeout,
			ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= cfg.ConsecutiveFailures },
			IsSuccessful: func(err error) bool {
				if err == nil {
					return true
				}
				if errors.Is(err, errSlow) {
					return false
				}
				return !isFailure(err)
			},
			OnStateChange: func(name string, from, to gobreaker.State) {
				observability.BreakerStateChange(name, int(fromGobreaker(to)), fromGobreaker(to).String())
				slog.Warn("breaker: cambio de estado", "breaker", name, "de", fromGobreaker(from).String(), "a", fromGobreaker(to).String())
			},
		}),
	}
}

func fromGobreaker(s gobreaker.State) State {
	switch s {
	case gobreaker.StateClosed:
		return Closed
	case gobreaker.StateHalfOpen:
		return HalfOpen
	default:
		return Open
	}
}

// State devuelve el estado actual (pasa de abierto a semiabierto al vencer OpenTimeout).
func (b *Breaker) State() State { return fromGobreaker(b.cb.State()) }

// Do ejecuta fn bajo el breaker. Si el circuito está abierto retorna ErrOpen sin ejecutar fn.
// Una llamada que termina bien pero supera SlowThreshold cuenta como fallo del circuito,
// aunque Do devuelve nil porque la operación sí se completó.
func (b *Breaker) Do(fn func() error) error {
	_, err := b.cb.Execute(func() (struct{}, error) {
		start := time.Now()
		err := fn()
		if err == nil && b.slow > 0 {
			if d := time.Since(start); d > b.slow {
				observability.BreakerSlowCall(b.name)
				slog.Warn("breaker: operación lenta, cuenta como fallo", "breaker", b.name, "duracion", d.Round(time.Millisecond), "umbral", b.slow)
				return struct{}{}, errSlow
			}
		}
		return struct{}{}, err
	})

	switch {
	case err == nil:
		return nil
	case errors.Is(err, errSlow):
		return nil
	case errors.Is(err, gobreaker.ErrOpenState), errors.Is(err, gobreaker.ErrTooManyRequests):
		observability.BreakerRejected(b.name)
		return fmt.Errorf("%w: %s", ErrOpen, b.name)
	default:
		return err
	}
}

// WaitReady bloquea mientras el circuito esté abierto. Si probe no es nil, al pasar a
// semiabierto se ejecuta como operación de prueba (p. ej. un Ping) y solo se retorna
// cuando el circuito vuelve a cerrarse. Retorna el error de ctx si se cancela antes.
func (b *Breaker) WaitReady(ctx context.Context, probe func(context.Context) error) error {
	for {
		switch b.State() {
		case Closed:
			return nil
		case HalfOpen:
			if probe == nil {
				return nil // la siguiente operación real hará de prueba
			}
			if err := b.Do(func() error { return probe(ctx) }); err != nil {
				slog.Debug("breaker: sonda fallida", "breaker", b.name, "error", err)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if b.State() == Closed {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}
