package worklease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/coordination"
)

type fakeClock struct {
	now   time.Time
	ticks chan time.Time
}

func (f *fakeClock) Now() time.Time                       { return f.now }
func (f *fakeClock) After(time.Duration) <-chan time.Time { return f.ticks }

type fakeStore struct {
	mu                         sync.Mutex
	lease                      coordination.Lease
	acquired                   bool
	acquireN, renewN, releaseN int
	renewErr                   error
	renewed                    chan struct{}
	renewWait                  bool
}

func (s *fakeStore) Acquire(context.Context, coordination.Assignment, string, time.Duration) (coordination.Lease, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquireN++
	return s.lease, s.acquired, nil
}
func (s *fakeStore) Renew(ctx context.Context, _ coordination.Lease, _ time.Duration) (coordination.Lease, error) {
	s.mu.Lock()
	s.renewN++
	wait := s.renewWait
	err := s.renewErr
	lease := s.lease
	if s.renewed != nil {
		s.renewed <- struct{}{}
	}
	s.mu.Unlock()
	if wait {
		<-ctx.Done()
		return coordination.Lease{}, ctx.Err()
	}
	return lease, err
}
func (s *fakeStore) Release(context.Context, coordination.Lease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseN++
	return nil
}
func (s *fakeStore) Advance(context.Context, coordination.Lease, coordination.Progress) error {
	return nil
}
func (s *fakeStore) Checkpoint(context.Context, coordination.Assignment) (coordination.Progress, bool, error) {
	return coordination.Progress{}, false, nil
}

func setup() (Supervisor, *fakeStore, *fakeClock) {
	now := time.Unix(100, 0)
	clock := &fakeClock{now: now, ticks: make(chan time.Time, 4)}
	store := &fakeStore{acquired: true, renewed: make(chan struct{}, 4), lease: coordination.Lease{Assignment: coordination.Assignment{Dataset: "d", Source: "s", Partition: "p"}, Holder: "h", Token: 1, ExpiresAt: now.Add(time.Minute)}}
	return Supervisor{Config: Config{Store: store, Assignment: store.lease.Assignment, Holder: "h", Duration: time.Second, Clock: clock, RenewalDelay: func(coordination.Lease, time.Time) time.Duration { return time.Millisecond }}}, store, clock
}

func TestRunCompletesAndReleases(t *testing.T) {
	s, st, _ := setup()
	if err := s.Run(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if st.acquireN != 1 || st.releaseN != 1 {
		t.Fatalf("acquire/release = %d/%d", st.acquireN, st.releaseN)
	}
}

func TestRunWithLeasePassesAcquiredIdentity(t *testing.T) {
	s, st, _ := setup()
	want := st.lease
	if err := s.RunWithLease(context.Background(), func(_ context.Context, got coordination.Lease) error {
		if got != want {
			t.Errorf("work lease = %+v, want %+v", got, want)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestRunContentionIsTyped(t *testing.T) {
	s, st, _ := setup()
	st.acquired = false
	err := s.Run(context.Background(), func(context.Context) error { return nil })
	var c *ContentionError
	if !errors.As(err, &c) || !errors.Is(err, ErrContended) || st.releaseN != 0 {
		t.Fatalf("err=%v release=%d", err, st.releaseN)
	}
}
func TestRunRenewsThenJoinsWork(t *testing.T) {
	s, st, clk := setup()
	started := make(chan struct{})
	finish := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- s.Run(context.Background(), func(context.Context) error { close(started); <-finish; return nil })
	}()
	<-started
	clk.ticks <- clk.now
	<-st.renewed
	close(finish)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if st.releaseN != 1 {
		t.Fatal("not released")
	}
}
func TestRenewFailureCancelsAndJoins(t *testing.T) {
	s, st, clk := setup()
	st.renewErr = coordination.ErrLeaseLost
	stopped := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- s.Run(context.Background(), func(ctx context.Context) error { <-ctx.Done(); close(stopped); return ctx.Err() })
	}()
	clk.ticks <- clk.now
	if err := <-result; !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatal(err)
	}
	<-stopped
	if st.releaseN != 1 {
		t.Fatal("not released")
	}
}
func TestParentCancellationCancelsAndJoins(t *testing.T) {
	s, st, _ := setup()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- s.Run(ctx, func(ctx context.Context) error { <-ctx.Done(); close(stopped); return nil })
	}()
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	<-stopped
	if st.releaseN != 1 {
		t.Fatal("not released")
	}
}
func TestPanicBecomesErrorAndReleases(t *testing.T) {
	s, st, _ := setup()
	err := s.Run(context.Background(), func(context.Context) error { panic("boom") })
	var p *PanicError
	if !errors.As(err, &p) || st.releaseN != 1 {
		t.Fatalf("%v release=%d", err, st.releaseN)
	}
}

func TestRenewalHasIndependentDeadline(t *testing.T) {
	now := time.Now()
	store := &fakeStore{acquired: true, renewWait: true, renewed: make(chan struct{}, 1), lease: coordination.Lease{
		Assignment: coordination.Assignment{Dataset: "d", Source: "s", Partition: "p"}, Holder: "h", Token: 1, ExpiresAt: now.Add(time.Second),
	}}
	s := Supervisor{Config: Config{
		Store: store, Assignment: store.lease.Assignment, Holder: "h", Duration: time.Second,
		RenewTimeout: 10 * time.Millisecond, RenewalDelay: func(coordination.Lease, time.Time) time.Duration { return time.Nanosecond },
	}}
	started := time.Now()
	err := s.Run(context.Background(), func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want renewal deadline", err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("renewal was not bounded: %s", time.Since(started))
	}
}
