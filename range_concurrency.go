package piko

const (
	minimumActiveConnections = 2
	initialProbeConnections  = 8
	concurrencyProbePartSize = 128 * 1024
)

type concurrencyProbe struct {
	generation int
	candidate  int
	samples    int
	seen       []bool
	done       bool
}

func newConcurrencyProbe(concurrency int) (int, concurrencyProbe) {
	candidate := min(concurrency, initialProbeConnections)
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

func (p *concurrencyProbe) allowsWorker(workerID int, maxActive int) bool {
	return !p.active() ||
		workerID >= 0 && workerID < min(maxActive, len(p.seen)) && !p.seen[workerID]
}

func (s *partScheduler) recordConcurrencyProbeLocked(workerID int, generation int) {
	if !s.probe.active() || s.rateLimited || generation != s.probe.generation ||
		workerID < 0 || workerID >= len(s.probe.seen) || s.probe.seen[workerID] {
		return
	}

	s.probe.seen[workerID] = true
	s.probe.samples++
	if s.probe.samples < s.probe.candidate {
		return
	}

	if s.probe.candidate < s.concurrency {
		s.startFullConcurrencyProbeLocked()
		return
	}
	s.probe.done = true
}

func (s *partScheduler) startFullConcurrencyProbeLocked() {
	s.maxActive = s.concurrency
	s.probe.candidate = s.concurrency
	s.probe.generation++
	s.probe.samples = 0
	clear(s.probe.seen)
}

func (s *partScheduler) stopConcurrencyProbeLocked() {
	s.probe.done = true
}
