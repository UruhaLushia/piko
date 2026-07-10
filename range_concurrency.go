package piko

import "time"

const (
	minimumActiveConnections   = 2
	concurrencyProbeConfirm    = 64 * 1024
	concurrencyProbeLowWindow  = time.Second
	concurrencyProbeHighWindow = 2 * time.Second
)

// Confirmed workers keep downloading while new slots probe the next concurrency level.
type concurrencyProbe struct {
	generation int
	candidate  int
	samples    int
	seen       []bool
	timer      *time.Timer
	done       bool
}

func newConcurrencyProbe(concurrency int) (int, concurrencyProbe) {
	candidate := min(concurrency, minimumActiveConnections)
	return candidate, concurrencyProbe{
		generation: 1,
		candidate:  candidate,
		seen:       make([]bool, concurrency),
		done:       concurrency <= minimumActiveConnections,
	}
}

func (p *concurrencyProbe) active() bool {
	return !p.done
}

func (p *concurrencyProbe) workerPending(workerID int) bool {
	return p.active() && workerID >= 0 && workerID < p.candidate && !p.seen[workerID]
}

func (s *partScheduler) confirmConcurrencyProbe(workerID int, generation int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.probe.active() || s.rateLimited || generation != s.probe.generation ||
		workerID < 0 || workerID >= len(s.probe.seen) || s.probe.seen[workerID] {
		return
	}

	s.probe.seen[workerID] = true
	s.probe.samples++
	if s.probe.samples >= s.probe.candidate {
		if s.probe.candidate < s.concurrency {
			s.expandConcurrencyProbeLocked()
			return
		}
		s.stopConcurrencyProbeLocked()
	}
}

func (s *partScheduler) expandConcurrencyProbeLocked() {
	s.probe.candidate = min(s.concurrency, s.probe.candidate*2)
	s.maxActive = s.probe.candidate
	s.probe.generation++
	s.resetConcurrencyProbeTimerLocked()
}

func (s *partScheduler) probeAllowsWorkerLocked(workerID int) bool {
	if !s.probe.active() {
		return true
	}
	if workerID < 0 || workerID >= s.probe.candidate {
		return false
	}
	if s.probe.workerPending(workerID) {
		return true
	}
	for pendingID := 0; pendingID < s.probe.candidate; pendingID++ {
		if s.probe.workerPending(pendingID) && s.active[pendingID] == nil {
			return false
		}
	}
	return true
}

func (s *partScheduler) limitConcurrencyProbeLocked() {
	s.maxActive = s.probe.observedLimit()
	s.closeExcessProbeConnectionsLocked(s.maxActive)
	s.stopConcurrencyProbeLocked()
}

func (p *concurrencyProbe) observedLimit() int {
	minimum := min(p.candidate, minimumActiveConnections)
	return min(p.candidate, max(minimum, p.samples))
}

func (s *partScheduler) stopConcurrencyProbeLocked() {
	if s.probe.timer != nil {
		s.probe.timer.Stop()
		s.probe.timer = nil
	}
	s.probe.done = true
}

func (s *partScheduler) startConcurrencyProbeTimer() {
	if !s.probe.active() {
		return
	}
	s.resetConcurrencyProbeTimerLocked()
}

func (s *partScheduler) resetConcurrencyProbeTimerLocked() {
	if s.probe.timer != nil {
		s.probe.timer.Stop()
	}
	generation := s.probe.generation
	window := concurrencyProbeLowWindow
	if s.probe.candidate > minimumActiveConnections*2 {
		window = concurrencyProbeHighWindow
	}
	s.probe.timer = time.AfterFunc(window, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.probe.active() || s.probe.generation != generation {
			return
		}
		s.limitConcurrencyProbeLocked()
		if s.maxActive < s.concurrency {
			s.rateLimited = true
			s.extendRecoveryLocked(time.Now(), rateLimitRecover)
		}
	})
}

func (s *partScheduler) closeExcessProbeConnectionsLocked(limit int) {
	kept := 0
	for workerID, active := range s.active {
		if active == nil || workerID >= len(s.probe.seen) || !s.probe.seen[workerID] {
			continue
		}
		if kept < limit {
			kept++
			continue
		}
		active.closeConnection()
	}
	for workerID, active := range s.active {
		if active != nil && (workerID >= len(s.probe.seen) || !s.probe.seen[workerID]) {
			active.closeConnection()
		}
	}
}
