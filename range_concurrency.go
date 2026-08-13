package piko

import (
	"sort"
	"time"
)

const (
	minimumActiveConnections = 2
	concurrencyProbeWindow   = time.Second
	concurrencyProbeRatio    = 0.35
)

// Startup uses all requested workers, then keeps only connections with
// sustained progress during the sampling window.
type concurrencyProbe struct {
	bytes      []int64
	enabled    []bool
	timer      *time.Timer
	timerEpoch int
	done       bool
}

func newConcurrencyProbe(concurrency int) (int, concurrencyProbe) {
	enabled := make([]bool, concurrency)
	for workerID := range enabled {
		enabled[workerID] = true
	}
	return concurrency, concurrencyProbe{
		bytes:   make([]int64, concurrency),
		enabled: enabled,
		done:    concurrency <= minimumActiveConnections,
	}
}

func (p *concurrencyProbe) active() bool {
	return !p.done
}

func (p *concurrencyProbe) workerPending(workerID int) bool {
	return p.active() && p.workerEnabled(workerID) && p.bytes[workerID] == 0
}

func (p *concurrencyProbe) workerEnabled(workerID int) bool {
	return workerID >= 0 && workerID < len(p.enabled) && p.enabled[workerID]
}

func (s *partScheduler) recordConcurrencyProbe(workerID int, bytes int64) {
	if bytes <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.probe.active() || s.rateLimited || !s.probe.workerEnabled(workerID) {
		return
	}
	s.probe.bytes[workerID] += bytes
	if s.probe.timer == nil {
		s.resetConcurrencyProbeTimerLocked()
	}
}

func (s *partScheduler) workerEnabled(workerID int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probe.workerEnabled(workerID)
}

func (s *partScheduler) probeAllowsWorkerLocked(workerID int) bool {
	return s.probe.workerEnabled(workerID)
}

func (s *partScheduler) limitConcurrencyProbeLocked() {
	s.setConcurrencyLimitLocked(s.probe.preferredLimit())
	s.stopConcurrencyProbeLocked()
}

func (p *concurrencyProbe) preferredLimit() int {
	total := int64(0)
	productive := 0
	for _, bytes := range p.bytes {
		if bytes > 0 {
			total += bytes
			productive++
		}
	}
	if productive == 0 {
		return min(len(p.enabled), minimumActiveConnections)
	}

	minimum := float64(total) / float64(productive) * concurrencyProbeRatio
	preferred := 0
	for _, bytes := range p.bytes {
		if float64(bytes) >= minimum {
			preferred++
		}
	}
	return min(len(p.enabled), max(preferred, min(len(p.enabled), minimumActiveConnections)))
}

func (s *partScheduler) setConcurrencyLimitLocked(limit int) {
	limit = min(s.concurrency, max(limit, min(s.concurrency, minimumActiveConnections)))
	s.probe.enabled = s.probe.fastestWorkers(limit)
	s.maxActive = limit
	s.closeExcessProbeConnectionsLocked(s.probe.enabled)
}

func (p *concurrencyProbe) fastestWorkers(limit int) []bool {
	workers := make([]int, len(p.bytes))
	for workerID := range workers {
		workers[workerID] = workerID
	}
	sort.SliceStable(workers, func(i, j int) bool {
		return p.bytes[workers[i]] > p.bytes[workers[j]]
	})

	enabled := make([]bool, len(p.enabled))
	for _, workerID := range workers[:min(limit, len(workers))] {
		enabled[workerID] = true
	}
	return enabled
}

func (s *partScheduler) stopConcurrencyProbeLocked() {
	if s.probe.timer != nil {
		s.probe.timer.Stop()
		s.probe.timer = nil
	}
	s.probe.done = true
}

func (s *partScheduler) resetConcurrencyProbeTimerLocked() {
	if s.probe.timer != nil {
		s.probe.timer.Stop()
	}
	s.probe.timerEpoch++
	timerEpoch := s.probe.timerEpoch
	s.probe.timer = time.AfterFunc(concurrencyProbeWindow, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.probe.active() || s.probe.timerEpoch != timerEpoch {
			return
		}
		s.limitConcurrencyProbeLocked()
		if s.maxActive < s.concurrency {
			s.rateLimited = true
			s.extendRecoveryLocked(time.Now(), rateLimitRecover)
		}
	})
}

func (s *partScheduler) closeExcessProbeConnectionsLocked(keep []bool) {
	for workerID, active := range s.active {
		if active != nil && (workerID >= len(keep) || !keep[workerID]) {
			active.closeConnection()
		}
	}
}
