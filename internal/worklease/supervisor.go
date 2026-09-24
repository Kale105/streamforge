// Package worklease runs one unit of work while it owns a coordination lease.
package worklease

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/Kale105/streamforge/internal/coordination"
)

// ErrContended is returned when another worker already owns the assignment.
var ErrContended = errors.New("lease is contended")

// ContentionError identifies the assignment that could not be acquired.
type ContentionError struct{ Assignment coordination.Assignment }

func (e *ContentionError) Error() string {
	return fmt.Sprintf("%v: %s/%s/%s", ErrContended, e.Assignment.Dataset, e.Assignment.Source, e.Assignment.Partition)
}
func (e *ContentionError) Unwrap() error { return ErrContended }

// PanicError is returned after a workload panic has been recovered and the
// lease has been released. The panic is deliberately not re-panicked because
// doing so would make the supervisor unable to provide its cleanup guarantee.
type PanicError struct{ Value any }

func (e *PanicError) Error() string { return fmt.Sprintf("lease workload panicked: %v", e.Value) }

// Clock is deliberately small so callers can test renewal without waiting.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RenewalDelay selects the next renewal delay. It must return a positive value
// that falls before lease expiry. It is principally a deterministic test seam.
type RenewalDelay func(lease coordination.Lease, now time.Time) time.Duration

// Config describes one ownership attempt.
type Config struct {
	Store          coordination.LeaseStore
	Assignment     coordination.Assignment
	Holder         string
	Duration       time.Duration
	CleanupTimeout time.Duration
	RenewTimeout   time.Duration
	Clock          Clock
	RenewalDelay   RenewalDelay
}

// Supervisor acquires and supervises leases using its Config.
type Supervisor struct{ Config }

// Run acquires exactly once, then executes work until it completes or the lease
// can no longer be renewed. It always waits for work to exit before returning.
func (s Supervisor) Run(ctx context.Context, work func(context.Context) error) (err error) {
	if work == nil {
		return errors.New("lease store and workload are required")
	}
	return s.RunWithLease(ctx, func(ctx context.Context, _ coordination.Lease) error { return work(ctx) })
}

// RunWithLease acquires exactly once and gives work the lease that established
// ownership. The value is deliberately the acquired lease, rather than a later
// renewal: its assignment and fencing token are stable identity for the whole
// workload and can safely be used to derive downstream fencing identities.
// Run remains available for workloads that do not need that identity.
func (s Supervisor) RunWithLease(ctx context.Context, work func(context.Context, coordination.Lease) error) (err error) {
	c := s.Config
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	if err := validate(c, work != nil); err != nil {
		return err
	}
	lease, acquired, err := c.Store.Acquire(ctx, c.Assignment, c.Holder, c.Duration)
	if err != nil {
		return err
	}
	if !acquired {
		return &ContentionError{Assignment: c.Assignment}
	}
	cleanup := c.CleanupTimeout
	if cleanup == 0 {
		cleanup = 5 * time.Second
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), cleanup)
		defer cancel()
		if releaseErr := c.Store.Release(releaseCtx, lease); err == nil && releaseErr != nil {
			err = releaseErr
		}
	}()

	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	acquiredLease := lease
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- &PanicError{Value: r}
			}
		}()
		done <- work(workCtx, acquiredLease)
	}()

	for {
		delay := renewalDelay(c, lease)
		timer := c.Clock.After(delay)
		select {
		case workErr := <-done:
			return workErr
		case <-ctx.Done():
			cancelWork()
			workErr := <-done
			if workErr != nil {
				return workErr
			}
			return ctx.Err()
		case <-timer:
			renewCtx, cancelRenew := context.WithTimeout(ctx, renewTimeout(c))
			next, renewErr := c.Store.Renew(renewCtx, lease, c.Duration)
			cancelRenew()
			if renewErr != nil {
				cancelWork()
				<-done
				return renewErr
			}
			lease = next
		}
	}
}

func validate(c Config, hasWork bool) error {
	if c.Store == nil || !hasWork {
		return errors.New("lease store and workload are required")
	}
	if err := c.Assignment.Validate(); err != nil {
		return err
	}
	if err := coordination.ValidateHolder(c.Holder); err != nil {
		return err
	}
	if err := coordination.ValidateDuration(c.Duration); err != nil {
		return err
	}
	if c.CleanupTimeout < 0 {
		return errors.New("cleanup timeout must not be negative")
	}
	if c.RenewTimeout < 0 || c.RenewTimeout >= c.Duration {
		return errors.New("renewal timeout must be non-negative and shorter than the lease duration")
	}
	return nil
}

func renewTimeout(c Config) time.Duration {
	if c.RenewTimeout > 0 {
		return c.RenewTimeout
	}
	timeout := c.Duration / 3
	if timeout > 5*time.Second {
		return 5 * time.Second
	}
	return timeout
}

func renewalDelay(c Config, lease coordination.Lease) time.Duration {
	now := c.Clock.Now()
	remaining := lease.ExpiresAt.Sub(now)
	if c.RenewalDelay != nil {
		d := c.RenewalDelay(lease, now)
		if d > 0 && d < remaining {
			return d
		}
		// An invalid injected policy renews immediately: safe and deterministic.
		return time.Nanosecond
	}
	// Renew near two thirds of the usable lease life. A small, bounded jitter
	// spreads otherwise synchronized workers, while still leaving a full third
	// of the observed remaining time for the store round trip.
	base := remaining * 2 / 3
	jitter := remaining / 12
	if jitter > 0 {
		base += time.Duration(rand.Int64N(int64(jitter*2+1))) - jitter
	}
	if base <= 0 {
		return time.Nanosecond
	}
	if base >= remaining {
		return remaining - time.Nanosecond
	}
	return base
}
