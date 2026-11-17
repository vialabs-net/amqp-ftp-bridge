package httpserver

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrBusy is returned when no FTP session became free within the wait.
var ErrBusy = errors.New("no free FTP session")

// Sessions caps the FTP sessions that HTTP requests hold at once. Each
// session has a slot number in [0, n), held by one request at a time.
type Sessions struct {
	slots chan int
	obs   Observer
}

// NewSessions returns a limiter of n sessions (at least one).
func NewSessions(n int, obs Observer) *Sessions {
	if n < 1 {
		n = 1
	}
	if obs == nil {
		obs = NopObserver{}
	}
	s := &Sessions{slots: make(chan int, n), obs: obs}
	for i := 0; i < n; i++ {
		s.slots <- i
	}
	return s
}

// Size returns the number of sessions.
func (s *Sessions) Size() int { return cap(s.slots) }

// Acquire waits up to wait for a free session. It returns the slot and a
// function that frees it, ErrBusy after the wait, or ctx's error.
func (s *Sessions) Acquire(ctx context.Context, wait time.Duration) (int, func(), error) {
	start := time.Now()
	var slot int
	select {
	case slot = <-s.slots:
	default:
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case slot = <-s.slots:
		case <-timer.C:
			s.obs.FTPSessionRejected()
			return 0, nil, ErrBusy
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
	s.obs.FTPSessionWait(time.Since(start))
	s.obs.FTPSessions(1)
	var once sync.Once
	return slot, func() {
		once.Do(func() {
			s.obs.FTPSessions(-1)
			s.slots <- slot
		})
	}, nil
}
