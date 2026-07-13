package piko

import (
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	idlePartPoll     = 50 * time.Millisecond
	minStealPartSize = 128 * 1024
	minStealAge      = 200 * time.Millisecond
)

type part struct {
	index            int
	start            int64
	end              int64
	requeues         int
	rateProbe        bool
	concurrencyProbe bool
}

func (p part) length() int64 {
	return p.end - p.start + 1
}

type partScheduler struct {
	initialPartSize int64
	maxPartSize     int64
	concurrency     int

	mu             sync.Mutex
	pending        []downloadSpan
	index          int
	workerDone     []int
	workerSize     []int64
	partSizeHint   int64
	activeCount    int
	maxActive      int
	probeLimit     int
	rateLimited    bool
	recoverAt      time.Time
	limitedAt      time.Time
	limitBackoffAt time.Time
	limitStrikes   int
	queue          []part
	delayed        []delayedPart
	active         []*activePart
	probe          concurrencyProbe
}

type delayedPart struct {
	part      part
	readyTime time.Time
}

type activePart struct {
	mu             sync.Mutex
	part           part
	started        time.Time
	probeID        int
	offset         atomic.Int64
	end            atomic.Int64
	connMu         sync.Mutex
	conn           net.Conn
	closeRequested bool
}

func newPartScheduler(size int64, initialPartSize int64, concurrency int, pending []downloadSpan) *partScheduler {
	if initialPartSize < 1 {
		initialPartSize = DefaultPartSize
	}
	if concurrency < 1 {
		concurrency = 1
	}
	maxPartSize := max(initialPartSize, min(int64(maxDynamicPartSize), initialPartSize*256))
	if size > 0 {
		maxPartSize = min(maxPartSize, size)
	}
	workerSize := make([]int64, concurrency)
	for i := range workerSize {
		workerSize[i] = initialPartSize
	}
	maxActive, probe := newConcurrencyProbe(concurrency)
	if pending == nil && size > 0 {
		pending = []downloadSpan{{start: 0, end: size - 1}}
	} else {
		pending = normalizeDownloadSpans(append([]downloadSpan(nil), pending...))
	}
	scheduler := &partScheduler{
		initialPartSize: initialPartSize,
		maxPartSize:     maxPartSize,
		concurrency:     concurrency,
		pending:         pending,
		workerDone:      make([]int, concurrency),
		workerSize:      workerSize,
		partSizeHint:    initialPartSize,
		maxActive:       maxActive,
		active:          make([]*activePart, concurrency),
		probe:           probe,
	}
	scheduler.startConcurrencyProbeTimer()
	return scheduler
}

func (p *activePart) setConnection(conn net.Conn) {
	p.connMu.Lock()
	if p.closeRequested {
		p.connMu.Unlock()
		closeConn(conn)
		return
	}
	p.conn = conn
	p.connMu.Unlock()
}

func (p *activePart) clearConnection() {
	p.connMu.Lock()
	p.conn = nil
	p.connMu.Unlock()
}

func (p *activePart) closeConnection() {
	p.connMu.Lock()
	p.closeRequested = true
	conn := p.conn
	p.conn = nil
	p.connMu.Unlock()
	closeConn(conn)
}

func (p *activePart) connectionCloseRequested() bool {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	return p.closeRequested
}

func (s *partScheduler) nextPart(workerID int) (*activePart, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recoverRateLimitLocked(time.Now())
	if s.activeCount >= s.maxActive || !s.probeAllowsWorkerLocked(workerID) {
		return nil, false
	}

	s.moveReadyDelayedLocked()
	if p, ok := s.popQueuedPartLocked(); ok {
		return s.activateNextLocked(workerID, p), true
	}

	p, ok := s.nextFreshPartLocked(workerID)
	if ok {
		return s.activateNextLocked(workerID, p), true
	}
	p, ok = s.stealPartLocked(workerID)
	if !ok {
		return nil, false
	}
	return s.activateNextLocked(workerID, p), true
}

func (s *partScheduler) popQueuedPartLocked() (part, bool) {
	if len(s.queue) == 0 {
		return part{}, false
	}
	last := len(s.queue) - 1
	p := s.queue[last]
	s.queue = s.queue[:last]
	return p, true
}

func (s *partScheduler) nextFreshPartLocked(workerID int) (part, bool) {
	if len(s.pending) == 0 {
		return part{}, false
	}
	index := s.index + 1
	remaining := downloadSpanBytes(s.pending)
	partSize := s.basePartSizeLocked(workerID, remaining)
	if index%2 == 0 {
		last := len(s.pending) - 1
		span := &s.pending[last]
		partSize = min(partSize, span.length())
		end := span.end
		start := end - partSize + 1
		span.end = start - 1
		if span.end < span.start {
			s.pending = s.pending[:last]
		}
		return part{start: start, end: end}, true
	}
	span := &s.pending[0]
	partSize = min(partSize, span.length())
	start := span.start
	end := start + partSize - 1
	span.start = end + 1
	if span.start > span.end {
		s.pending = s.pending[1:]
	}
	return part{start: start, end: end}, true
}

func (s *partScheduler) activateNextLocked(workerID int, p part) *activePart {
	s.index++
	p.index = s.index
	p.rateProbe = s.rateProbeLocked()
	p.concurrencyProbe = s.probe.workerPending(workerID)
	return s.activateLocked(workerID, p)
}

func (s *partScheduler) activateLocked(workerID int, p part) *activePart {
	active := &activePart{part: p, started: time.Now(), probeID: s.probe.generation}
	active.offset.Store(p.start)
	active.end.Store(p.end)

	if workerID >= 0 && workerID < len(s.active) {
		if s.active[workerID] == nil {
			s.activeCount++
		}
		s.active[workerID] = active
	}
	return active
}

func (s *partScheduler) finish(workerID int, active *activePart) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if workerID >= 0 && workerID < len(s.active) && s.active[workerID] == active {
		s.active[workerID] = nil
		if s.activeCount > 0 {
			s.activeCount--
		}
	}
}

func (s *partScheduler) hasPendingWork() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.moveReadyDelayedLocked()
	if len(s.queue) > 0 || len(s.delayed) > 0 || len(s.pending) > 0 {
		return true
	}
	for _, active := range s.active {
		if active != nil {
			return true
		}
	}
	return false
}

func (s *partScheduler) stealPartLocked(workerID int) (part, bool) {
	var chosen *activePart
	var chosenRemaining int64

	now := time.Now()
	for id, active := range s.active {
		if id == workerID || active == nil || now.Sub(active.started) < minStealAge {
			continue
		}

		active.mu.Lock()
		remaining := active.end.Load() - active.offset.Load() + 1
		active.mu.Unlock()
		if remaining < minStealPartSize*2 || remaining <= chosenRemaining {
			continue
		}
		chosen = active
		chosenRemaining = remaining
	}
	if chosen == nil {
		return part{}, false
	}

	chosen.mu.Lock()
	defer chosen.mu.Unlock()

	oldEnd := chosen.end.Load()
	start := chosen.offset.Load()
	remaining := oldEnd - start + 1
	if remaining < minStealPartSize*2 {
		return part{}, false
	}

	stolen := part{
		requeues: chosen.part.requeues,
		end:      oldEnd,
	}
	stolen.start = start + remaining/2
	chosen.end.Store(stolen.start - 1)
	return stolen, true
}

func (s *partScheduler) requeue(p part, offset int64, maxRequeues int, delay time.Duration) bool {
	if offset > p.end {
		return false
	}
	if maxRequeues < 0 {
		maxRequeues = 0
	}
	p.start = offset
	p.requeues++
	p.rateProbe = false
	if p.requeues > maxRequeues+1 {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if delay > 0 {
		s.delayed = append(s.delayed, delayedPart{part: p, readyTime: time.Now().Add(delay)})
		return true
	}

	chunkSize := p.length() >> 1
	chunkSize = max(chunkSize, s.initialPartSize)
	chunkSize = max(chunkSize, int64(minDynamicPartSize))
	chunkSize = min(chunkSize, p.length())

	chunks := make([]part, 0, (p.length()+chunkSize-1)/chunkSize)
	for start := p.start; start <= p.end; {
		end := min(start+chunkSize-1, p.end)
		chunks = append(chunks, part{start: start, end: end, requeues: p.requeues})
		start = end + 1
	}
	for _, chunk := range slices.Backward(chunks) {
		s.queue = append(s.queue, chunk)
	}
	return true
}

func (s *partScheduler) moveReadyDelayedLocked() {
	if len(s.delayed) == 0 {
		return
	}
	now := time.Now()
	pending := s.delayed[:0]
	for _, delayed := range s.delayed {
		if now.Before(delayed.readyTime) {
			pending = append(pending, delayed)
			continue
		}
		s.queue = append(s.queue, delayed.part)
	}
	s.delayed = pending
}
