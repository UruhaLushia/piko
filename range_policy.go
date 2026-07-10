package piko

import (
	"sync/atomic"
	"time"
)

const (
	rangeLease         = 12 * time.Second
	minLeasedPartSpeed = 64 * 1024
)

type rangeRetryPlan struct {
	maxRequeues int
	delay       time.Duration
}

func (d *downloader) planRangeRetry(scheduler *partScheduler, workerID int, p part, offset int64, partSize int64, err error) rangeRetryPlan {
	plan := rangeRetryPlan{maxRequeues: max(d.retries*4, 8)}
	switch {
	case isRateLimitedDownloadError(err):
		plan.maxRequeues = max(d.retries*16, 64)
		plan.delay = rateLimitDelay(p.requeues)
		scheduler.limitForRateLimit(plan.delay)
		if p.end-offset+1 <= max(partSize, int64(minDynamicPartSize*2)) {
			plan.delay = 0
		}
	case isRateProbeTimeout(err):
		plan.maxRequeues = max(d.retries*16, 64)
		scheduler.rejectRateProbe(rateLimitRecover)
	case isTransientRangeError(err):
		plan.maxRequeues = max(d.retries*24, 96)
		if p.requeues >= d.retries {
			plan.delay = retryDelay(p.requeues - d.retries)
		}
		scheduler.penalize(workerID)
	default:
		scheduler.penalize(workerID)
	}
	return plan
}

func (p part) probeIdleTimeout() time.Duration {
	if p.concurrencyProbe {
		return concurrencyProbeHighWindow
	}
	if !p.rateProbe {
		return 0
	}
	return rateLimitIdle
}

type idleTimer struct {
	timer    *time.Timer
	timedOut atomic.Bool
}

func newIdleTimer(timeout time.Duration, onTimeout func()) *idleTimer {
	if timeout <= 0 {
		return nil
	}
	probe := &idleTimer{}
	probe.timer = time.AfterFunc(timeout, func() {
		probe.timedOut.Store(true)
		onTimeout()
	})
	return probe
}

func (p *idleTimer) stop() {
	if p != nil {
		p.timer.Stop()
	}
}

func (p *idleTimer) reset(timeout time.Duration) {
	if p != nil {
		resetTimer(p.timer, timeout)
	}
}

func (p *idleTimer) expired() bool {
	return p != nil && p.timedOut.Load()
}

func partLease(p part) time.Duration {
	if p.requeues == 0 && p.length() > slowTailWindow {
		return 0
	}
	if p.length() > DefaultPartSize*4 {
		return 0
	}

	lease := rangeLease
	if p.length() <= slowTailWindow {
		lease = time.Duration(p.length()*int64(time.Second)) / minLeasedPartSpeed
	}
	if p.length() <= minDynamicPartSize && lease < rangeLease/2 {
		lease = rangeLease / 2
	}
	if p.requeues > 0 {
		lease = lease / time.Duration(p.requeues+1)
	}
	if lease < 4*time.Second {
		return 4 * time.Second
	}
	return lease
}

func startLeaseMonitor(cancel func(), lease time.Duration) func() {
	if lease <= 0 {
		return func() {}
	}

	done := make(chan struct{})
	timer := time.NewTimer(lease)
	go func() {
		select {
		case <-timer.C:
			cancel()
		case <-done:
		}
	}()

	return func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		close(done)
	}
}
