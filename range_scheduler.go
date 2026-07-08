package piko

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	rangeLease         = 8 * time.Second
	maxDynamicPartSize = 128 * 1024 * 1024
	minDynamicPartSize = 512 * 1024
	minTailPartSize    = 128 * 1024
	minStealPartSize   = 64 * 1024
	minStealAge        = 200 * time.Millisecond
	idlePartPoll       = 50 * time.Millisecond
	tailPartsPerConn   = 4
	speedSmoothFactor  = 0.35
	partSizeTargetTime = 8 * time.Second
)

type partScheduler struct {
	initialPartSize int64
	maxPartSize     int64
	concurrency     int

	mu          sync.Mutex
	front       int64
	back        int64
	index       int
	workerDone  []int
	workerSpeed []float64
	workerSize  []int64
	queue       []part
	delayed     []delayedPart
	active      []*activePart
}

type delayedPart struct {
	part      part
	readyTime time.Time
}

type activePart struct {
	mu       sync.Mutex
	cancelMu sync.Mutex
	part     part
	started  time.Time
	offset   atomic.Int64
	end      atomic.Int64
	cancel   context.CancelFunc
	cancelID int64
}

func (p *activePart) setCancel(cancel context.CancelFunc) int64 {
	p.cancelMu.Lock()
	defer p.cancelMu.Unlock()
	p.cancelID++
	p.cancel = cancel
	return p.cancelID
}

func (p *activePart) clearCancel(id int64) {
	p.cancelMu.Lock()
	defer p.cancelMu.Unlock()
	if p.cancelID == id {
		p.cancel = nil
	}
}

func (p *activePart) cancelAttempt() {
	p.cancelMu.Lock()
	cancel := p.cancel
	p.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func newPartScheduler(size int64, initialPartSize int64, concurrency int) *partScheduler {
	if initialPartSize < 1 {
		initialPartSize = DefaultPartSize
	}
	if concurrency < 1 {
		concurrency = 1
	}
	maxPartSize := max(initialPartSize, min(int64(maxDynamicPartSize), initialPartSize*32))
	if size > 0 {
		maxPartSize = min(maxPartSize, size)
	}
	workerSize := make([]int64, concurrency)
	for i := range workerSize {
		workerSize[i] = initialPartSize
	}
	return &partScheduler{
		initialPartSize: initialPartSize,
		maxPartSize:     maxPartSize,
		concurrency:     concurrency,
		back:            size - 1,
		workerDone:      make([]int, concurrency),
		workerSpeed:     make([]float64, concurrency),
		workerSize:      workerSize,
		active:          make([]*activePart, concurrency),
	}
}

func (s *partScheduler) nextPart(workerID int) (part, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.moveReadyDelayedLocked()
	if len(s.queue) > 0 {
		last := len(s.queue) - 1
		p := s.queue[last]
		s.queue = s.queue[:last]
		s.index++
		p.index = s.index
		return p, true
	}

	if s.front > s.back {
		return s.stealPartLocked(workerID)
	}

	remaining := s.back - s.front + 1
	partSize := s.nextPartSizeLocked(workerID, remaining)
	s.index++

	var start, end int64
	if s.index%2 == 0 {
		end = s.back
		start = max(end-partSize+1, s.front)
		s.back = start - 1
	} else {
		start = s.front
		end = min(start+partSize-1, s.back)
		s.front = end + 1
	}

	return part{index: s.index, start: start, end: end}, true
}

func (s *partScheduler) activate(workerID int, p part) *activePart {
	active := &activePart{part: p, started: time.Now()}
	active.offset.Store(p.start)
	active.end.Store(p.end)

	s.mu.Lock()
	if workerID >= 0 && workerID < len(s.active) {
		s.active[workerID] = active
	}
	s.mu.Unlock()
	return active
}

func (s *partScheduler) finish(workerID int, active *activePart) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if workerID >= 0 && workerID < len(s.active) && s.active[workerID] == active {
		s.active[workerID] = nil
	}
}

func (s *partScheduler) hasInFlight() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.moveReadyDelayedLocked()
	if len(s.queue) > 0 || len(s.delayed) > 0 || s.front <= s.back {
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
		if id == workerID || active == nil {
			continue
		}
		if now.Sub(active.started) < minStealAge {
			continue
		}

		active.mu.Lock()
		end := active.end.Load()
		start := active.offset.Load()
		remaining := end - start + 1
		active.mu.Unlock()
		if remaining < minStealPartSize {
			continue
		}
		if remaining > chosenRemaining {
			chosen = active
			chosenRemaining = remaining
		}
	}
	if chosen == nil {
		return part{}, false
	}

	chosen.mu.Lock()
	defer chosen.mu.Unlock()

	oldEnd := chosen.end.Load()
	start := chosen.offset.Load()
	remaining := oldEnd - start + 1
	if remaining < minStealPartSize {
		return part{}, false
	}

	stolen := part{
		requeues: chosen.part.requeues + 1,
		end:      oldEnd,
	}
	handoff := remaining < minStealPartSize*2
	if handoff {
		chosen.end.Store(start - 1)
		stolen.start = start
	} else {
		splitStart := start + remaining/2
		chosen.end.Store(splitStart - 1)
		stolen.start = splitStart
	}

	s.index++
	stolen.index = s.index
	if handoff {
		chosen.cancelAttempt()
	}
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
	for i := len(chunks) - 1; i >= 0; i-- {
		s.queue = append(s.queue, chunks[i])
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

func (s *partScheduler) record(workerID int, bytes int64, elapsed time.Duration) {
	if workerID < 0 || workerID >= len(s.workerSpeed) || bytes <= 0 || elapsed <= 0 {
		return
	}

	speed := float64(bytes) / elapsed.Seconds()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.workerSpeed[workerID] == 0 {
		s.workerSpeed[workerID] = speed
	} else {
		s.workerSpeed[workerID] = s.workerSpeed[workerID]*(1-speedSmoothFactor) + speed*speedSmoothFactor
	}
	s.adjustPartSizeLocked(workerID, bytes, elapsed)
	s.workerDone[workerID]++
}

func (s *partScheduler) penalize(workerID int) {
	if workerID < 0 || workerID >= len(s.workerSize) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.workerSize[workerID]
	if current <= 0 {
		current = s.initialPartSize
	}
	s.workerSize[workerID] = max(current/2, int64(minDynamicPartSize))
}

func (s *partScheduler) nextPartSizeLocked(workerID int, remaining int64) int64 {
	return s.basePartSizeLocked(workerID, remaining)
}

func (s *partScheduler) basePartSizeLocked(workerID int, remaining int64) int64 {
	if remaining > s.tailWindow() {
		return min(remaining, s.workerPartSizeLocked(workerID))
	}

	targetParts := int64(s.concurrency) * tailPartsPerConn
	partSize := (remaining + targetParts - 1) / targetParts
	return clampPartSize(partSize, remaining, s.initialPartSize, minTailPartSize)
}

func (s *partScheduler) tailWindow() int64 {
	return max(s.initialPartSize*8, int64(s.concurrency)*minDynamicPartSize*2)
}

func (s *partScheduler) workerPartSizeLocked(workerID int) int64 {
	if workerID >= 0 && workerID < len(s.workerSize) && s.workerSize[workerID] > 0 {
		return s.workerSize[workerID]
	}
	return s.initialPartSize
}

func (s *partScheduler) adjustPartSizeLocked(workerID int, bytes int64, elapsed time.Duration) {
	if workerID < 0 || workerID >= len(s.workerSize) || bytes < min(s.initialPartSize, s.workerPartSizeLocked(workerID)/2) {
		return
	}
	current := s.workerPartSizeLocked(workerID)
	target := int64(float64(bytes) / elapsed.Seconds() * partSizeTargetTime.Seconds())
	target = clampPartSize(target, s.maxPartSize, s.maxPartSize, minDynamicPartSize)
	switch {
	case target > current:
		s.workerSize[workerID] = min(target, current*2)
	case target < current/2:
		s.workerSize[workerID] = max(target, current/2)
	default:
		s.workerSize[workerID] = (current + target) / 2
	}
}

func clampPartSize(size int64, remaining int64, maxPartSize int64, minPartSize int64) int64 {
	if maxPartSize < 1 {
		maxPartSize = DefaultPartSize
	}
	if minPartSize < 1 {
		minPartSize = minDynamicPartSize
	}
	size = max(size, minPartSize)
	size = min(size, maxPartSize)
	return min(size, remaining)
}

func partLease(p part) time.Duration {
	if p.requeues == 0 && p.length() > minDynamicPartSize {
		return 0
	}
	if p.length() > DefaultPartSize*4 {
		return 0
	}

	lease := rangeLease
	if p.length() <= minDynamicPartSize {
		lease = rangeLease / 2
	}
	if p.requeues > 0 {
		lease = lease / time.Duration(p.requeues+1)
	}
	if lease < 2*time.Second {
		return 2 * time.Second
	}
	return lease
}

func startLeaseMonitor(cancel context.CancelFunc, lease time.Duration) func() {
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
