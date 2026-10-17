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
	minStealPartSize = 64 * 1024
	minStealAge      = 200 * time.Millisecond
	tailHandoffAge   = time.Second
	tailHandoffRatio = 0.35
	workerRateSmooth = 0.35
)

type part struct {
	index            int
	start            int64
	end              int64
	requeues         int
	slowRetries      int
	allowSlow        bool
	rateProbe        bool
	concurrencyProbe bool
	source           int
}

func (p part) length() int64 {
	return p.end - p.start + 1
}

type partScheduler struct {
	initialPartSize int64
	maxPartSize     int64
	concurrency     int
	strategy        ConcurrencyStrategy

	mu             sync.Mutex
	pending        []downloadSpan
	index          int
	workerDone     []int
	workerSize     []int64
	workerRate     []float64
	workerLatency  []time.Duration
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
	sample         func() bool
	part           part
	started        time.Time
	bodyStarted    time.Time
	bodyOffset     int64
	bodyWriteNanos int64
	writeNanos     atomic.Int64
	writing        atomic.Bool
	startup        time.Duration
	ready          bool
	recentRate     float64
	recentRateAt   time.Time
	offset         atomic.Int64
	end            atomic.Int64
	connMu         sync.Mutex
	conn           net.Conn
	closeRequested bool
	retire         func(net.Conn)
}

func newPartScheduler(size int64, initialPartSize int64, concurrency int, pending []downloadSpan, strategy ConcurrencyStrategy) *partScheduler {
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
	if strategy != ConcurrencyStrategyAdaptive {
		maxActive = concurrency
		probe.done = true
	}
	if pending == nil && size > 0 {
		pending = []downloadSpan{{start: 0, end: size - 1}}
	} else {
		pending = normalizeDownloadSpans(append([]downloadSpan(nil), pending...))
	}
	scheduler := &partScheduler{
		initialPartSize: initialPartSize,
		maxPartSize:     maxPartSize,
		concurrency:     concurrency,
		strategy:        strategy,
		pending:         pending,
		workerDone:      make([]int, concurrency),
		workerSize:      workerSize,
		workerRate:      make([]float64, concurrency),
		workerLatency:   make([]time.Duration, concurrency),
		partSizeHint:    initialPartSize,
		maxActive:       maxActive,
		active:          make([]*activePart, concurrency),
		probe:           probe,
	}
	return scheduler
}

func (p *activePart) setConnection(conn net.Conn) {
	p.connMu.Lock()
	if p.closeRequested {
		retire := p.retire
		p.connMu.Unlock()
		if retire != nil {
			retire(conn)
		} else {
			closeConn(conn)
		}
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
	retire := p.retire
	p.conn = nil
	p.connMu.Unlock()
	if retire != nil {
		retire(conn)
	} else {
		closeConn(conn)
	}
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
	if s.activeCount >= s.maxActive || !s.probe.workerEnabled(workerID) {
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
	active := &activePart{part: p, started: time.Now()}
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
	var chosenID int
	var chosenRemaining int64
	var chosenETA float64

	now := time.Now()
	for id, active := range s.active {
		if id == workerID || active == nil {
			continue
		}

		active.mu.Lock()
		started, minimumAge := active.bodyStarted, minStealAge
		if started.IsZero() {
			started = active.started
			minimumAge += active.startup
		}
		if !active.ready || active.writing.Load() || now.Sub(started) < minimumAge {
			active.mu.Unlock()
			continue
		}
		remaining := active.end.Load() - active.offset.Load() + 1
		speed := activePartRate(active, id, s.workerRate, now)
		if !s.stealWorthwhileLocked(workerID, remaining, speed) {
			active.mu.Unlock()
			continue
		}
		active.mu.Unlock()
		if remaining < minStealPartSize*2 && !s.canHandoffTailLocked(active, workerID, remaining, speed, now) {
			continue
		}
		eta := float64(remaining) / max(speed, 1)
		if chosen != nil && (eta < chosenETA || eta == chosenETA && remaining <= chosenRemaining) {
			continue
		}
		chosen = active
		chosenID = id
		chosenRemaining = remaining
		chosenETA = eta
	}
	if chosen == nil {
		return part{}, false
	}

	chosen.mu.Lock()
	defer chosen.mu.Unlock()

	oldEnd := chosen.end.Load()
	start := chosen.offset.Load()
	remaining := oldEnd - start + 1
	if chosen.writing.Load() {
		return part{}, false
	}
	victimRate := activePartRate(chosen, chosenID, s.workerRate, time.Now())
	if !s.stealWorthwhileLocked(workerID, remaining, victimRate) {
		return part{}, false
	}
	if remaining < minStealPartSize*2 {
		if !s.canHandoffTailLocked(chosen, workerID, remaining, victimRate, time.Now()) {
			return part{}, false
		}
		chosen.end.Store(start - 1)
		chosen.closeConnection()
		return part{
			start:       start,
			end:         oldEnd,
			requeues:    chosen.part.requeues,
			slowRetries: chosen.part.slowRetries,
			allowSlow:   chosen.part.allowSlow,
		}, true
	}
	stolen := part{
		requeues:    chosen.part.requeues,
		slowRetries: chosen.part.slowRetries,
		allowSlow:   chosen.part.allowSlow,
		end:         oldEnd,
	}
	helperRate := s.workerRateForLocked(workerID)
	stolenSize := splitPartSize(remaining, victimRate, helperRate, s.workerLatency[workerID])
	stolen.start = oldEnd - stolenSize + 1
	chosen.end.Store(stolen.start - 1)
	return stolen, true
}

func (s *partScheduler) canHandoffTailLocked(active *activePart, helperID int, remaining int64, victimRate float64, now time.Time) bool {
	if remaining <= 0 || remaining >= minStealPartSize*2 ||
		now.Sub(active.started) < tailHandoffAge {
		return false
	}
	helperRate := s.workerRateForLocked(helperID)
	if helperRate <= 0 {
		return false
	}
	return victimRate <= 0 || victimRate < helperRate*tailHandoffRatio
}

func (s *partScheduler) stealWorthwhileLocked(workerID int, remaining int64, victimRate float64) bool {
	if remaining <= 0 {
		return false
	}
	helperRate := s.workerRateForLocked(workerID)
	if helperRate <= 0 {
		return false
	}
	if victimRate <= 0 {
		return true
	}
	startup := s.workerLatency[workerID]
	stolen := remaining
	if remaining >= minStealPartSize*2 {
		stolen = splitPartSize(remaining, victimRate, helperRate, startup)
	}
	finish := max(float64(remaining-stolen)/victimRate, startup.Seconds()+float64(stolen)/helperRate)
	return float64(remaining)/victimRate-finish >= minStealAge.Seconds()
}

func activePartRate(active *activePart, workerID int, workerRate []float64, now time.Time) float64 {
	if !active.bodyStarted.IsZero() && now.Sub(active.bodyStarted) >= minStealAge &&
		active.recentRateAt.After(active.bodyStarted) && now.Sub(active.recentRateAt) <= slowConnectionCheckInterval {
		return active.recentRate
	}
	if !active.bodyStarted.IsZero() && now.Sub(active.bodyStarted) >= minStealAge {
		elapsed := max(now.Sub(active.bodyStarted)-time.Duration(active.writeNanos.Load()-active.bodyWriteNanos), time.Nanosecond).Seconds()
		progress := active.offset.Load() - active.bodyOffset
		if progress > 0 {
			return float64(progress) / elapsed
		}
	}
	if workerID >= 0 && workerID < len(workerRate) {
		return workerRate[workerID]
	}
	return 0
}

func (s *partScheduler) workerRateForLocked(workerID int) float64 {
	if workerID >= 0 && workerID < len(s.workerRate) && s.workerRate[workerID] > 0 {
		return s.workerRate[workerID]
	}
	var total float64
	var count int
	for _, rate := range s.workerRate {
		if rate > 0 {
			total += rate
			count++
		}
	}
	if count > 0 {
		return total / float64(count)
	}
	return 0
}

func splitPartSize(remaining int64, victimRate float64, helperRate float64, startup time.Duration) int64 {
	stolen := remaining / 2
	if victimRate <= 0 && helperRate > 0 {
		stolen = remaining - minStealPartSize
	} else if victimRate > 0 && helperRate > 0 {
		stolen = int64((float64(remaining) - victimRate*startup.Seconds()) * helperRate / (victimRate + helperRate))
	}
	return min(max(stolen, int64(minStealPartSize)), remaining-minStealPartSize)
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
		chunks = append(chunks, part{
			start:       start,
			end:         end,
			requeues:    p.requeues,
			slowRetries: p.slowRetries,
			allowSlow:   p.allowSlow,
		})
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
