package piko

import "time"

const (
	maxDynamicPartSize = 1024 * 1024 * 1024
	warmupPartSize     = 4 * 1024 * 1024
	minDynamicPartSize = 512 * 1024
	minTailPartSize    = 128 * 1024
	tailPartsPerConn   = 4
	limitedTailParts   = 2
	partSizeTargetTime = 24 * time.Second
	rateLimitedPartMin = 32 * 1024 * 1024
)

func (s *partScheduler) record(workerID int, bytes int64, elapsed time.Duration, latency time.Duration) {
	if workerID < 0 || workerID >= len(s.workerSize) || bytes <= 0 || elapsed <= 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.adjustPartSizeLocked(workerID, bytes, elapsed)
	s.updatePartSizeHintLocked(s.workerPartSizeLocked(workerID))
	s.updateWorkerRateLocked(workerID, float64(bytes)/elapsed.Seconds())
	s.workerLatency[workerID] = latency
	s.workerDone[workerID]++
}

func (s *partScheduler) updateWorkerRateLocked(workerID int, rate float64) {
	if workerID < 0 || workerID >= len(s.workerRate) || rate <= 0 {
		return
	}
	if s.workerRate[workerID] <= 0 {
		s.workerRate[workerID] = rate
		return
	}
	s.workerRate[workerID] = s.workerRate[workerID]*(1-workerRateSmooth) + rate*workerRateSmooth
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

func (s *partScheduler) basePartSizeLocked(workerID int, remaining int64) int64 {
	if s.probe.workerPending(workerID) {
		return min(remaining, min(s.maxPartSize, min(s.initialPartSize, int64(warmupPartSize))))
	}
	if s.shouldWarmupLocked(workerID) {
		return min(remaining, min(s.maxPartSize, int64(warmupPartSize)))
	}

	if remaining > s.tailWindowLocked() {
		return min(remaining, s.workerPartSizeLocked(workerID))
	}

	targetParts := int64(s.effectiveConcurrencyLocked()) * s.tailPartsPerConnLocked()
	partSize := (remaining + targetParts - 1) / targetParts
	if workerID >= 0 && workerID < len(s.workerRate) {
		// Divide startup-heavy work among workers still free to take new parts.
		startupBytes := int64(s.workerRate[workerID] * s.workerLatency[workerID].Seconds() * 4)
		waiting := int64(max(s.effectiveConcurrencyLocked()-s.activeCount, 1))
		fairShare := (remaining + waiting - 1) / waiting
		partSize = max(partSize, min(startupBytes, fairShare))
	}
	return clampPartSize(partSize, remaining, s.maxPartSize, minTailPartSize)
}

func (s *partScheduler) prepare(workerID int, active *activePart, rate float64, latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rate <= 0 || workerID < 0 || workerID >= len(s.workerRate) {
		return
	}
	s.workerRate[workerID] = rate
	s.workerLatency[workerID] = latency
	if active.part.rateProbe || active.part.concurrencyProbe {
		return
	}
	remaining := downloadSpanBytes(s.pending)
	for _, p := range s.queue {
		remaining += p.length()
	}
	if remaining == 0 || remaining > s.tailWindowLocked() {
		return
	}
	// A new source can have a very different startup cost from this worker's last one.
	target := min(s.maxPartSize, max(active.part.length(), int64(rate*latency.Seconds()*4)))
	for active.part.length() < target {
		grew := false
		for i := range s.pending {
			if active.part.requeues != 0 || active.part.slowRetries != 0 || active.part.allowSlow {
				break
			}
			span := &s.pending[i]
			if growPart(&active.part, &span.start, &span.end, target) {
				if span.length() == 0 {
					s.pending = append(s.pending[:i], s.pending[i+1:]...)
				}
				grew = true
				break
			}
		}
		if grew {
			continue
		}
		for i := range s.queue {
			p := &s.queue[i]
			if p.rateProbe || p.concurrencyProbe || p.requeues != active.part.requeues || p.slowRetries != active.part.slowRetries || p.allowSlow != active.part.allowSlow {
				continue
			}
			if growPart(&active.part, &p.start, &p.end, target) {
				if p.length() == 0 {
					s.queue = append(s.queue[:i], s.queue[i+1:]...)
				}
				grew = true
				break
			}
		}
		if !grew {
			break
		}
	}
	active.offset.Store(active.part.start)
	active.end.Store(active.part.end)
}

func growPart(p *part, start, end *int64, target int64) bool {
	extra := min(target-p.length(), *end-*start+1)
	switch {
	case extra <= 0:
		return false
	case *start == p.end+1:
		p.end += extra
		*start += extra
	case *end+1 == p.start:
		p.start -= extra
		*end -= extra
	default:
		return false
	}
	return true
}

func (s *partScheduler) shouldWarmupLocked(workerID int) bool {
	if s.rateLimited {
		return false
	}
	return workerID >= 0 && workerID < len(s.workerDone) && s.workerDone[workerID] == 0
}

func (s *partScheduler) tailWindowLocked() int64 {
	return max(s.initialPartSize*16, int64(s.effectiveConcurrencyLocked())*s.initialPartSize)
}

func (s *partScheduler) effectiveConcurrencyLocked() int {
	s.normalizeMaxActiveLocked()
	return max(s.maxActive, 1)
}

func (s *partScheduler) tailPartsPerConnLocked() int64 {
	if s.rateLimited {
		return limitedTailParts
	}
	return tailPartsPerConn
}

func (s *partScheduler) workerPartSizeLocked(workerID int) int64 {
	size := int64(0)
	if workerID >= 0 && workerID < len(s.workerSize) && s.workerSize[workerID] > 0 {
		size = s.workerSize[workerID]
	}
	if size <= 0 {
		size = s.initialPartSize
	}
	if s.rateLimited {
		size = max(size, s.partSizeHint, s.rateLimitedPartFloorLocked())
	}
	return min(size, s.maxPartSize)
}

func (s *partScheduler) adjustPartSizeLocked(workerID int, bytes int64, elapsed time.Duration) {
	if workerID < 0 || workerID >= len(s.workerSize) || bytes < min(s.initialPartSize, s.workerPartSizeLocked(workerID)/2) {
		return
	}
	current := s.workerPartSizeLocked(workerID)
	target := int64(float64(bytes) / elapsed.Seconds() * partSizeTargetTime.Seconds())
	target = clampPartSize(target, s.maxPartSize, s.maxPartSize, minDynamicPartSize)
	if s.workerDone[workerID] == 0 {
		s.workerSize[workerID] = target
		return
	}
	switch {
	case target > current:
		s.workerSize[workerID] = min(target, current*4)
	case target < current/2:
		s.workerSize[workerID] = max(target, current/2)
	default:
		s.workerSize[workerID] = (current + target) / 2
	}
}

func (s *partScheduler) updatePartSizeHintLocked(size int64) {
	if size <= 0 {
		return
	}
	if s.partSizeHint <= s.initialPartSize {
		s.partSizeHint = size
		return
	}
	s.partSizeHint = (s.partSizeHint*2 + size) / 3
}

func (s *partScheduler) rateLimitedPartFloorLocked() int64 {
	return min(max(s.initialPartSize, int64(rateLimitedPartMin)), s.maxPartSize)
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
