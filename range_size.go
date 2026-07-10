package piko

import "time"

const (
	maxDynamicPartSize = 1024 * 1024 * 1024
	warmupPartSize     = 4 * 1024 * 1024
	minDynamicPartSize = 512 * 1024
	minTailPartSize    = 128 * 1024
	startupActive      = 4
	tailPartsPerConn   = 4
	limitedTailParts   = 2
	partSizeTargetTime = 24 * time.Second
	rateLimitedPartMin = 32 * 1024 * 1024
)

func (s *partScheduler) record(workerID int, bytes int64, elapsed time.Duration) {
	if workerID < 0 || workerID >= len(s.workerSize) || bytes <= 0 || elapsed <= 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.adjustPartSizeLocked(workerID, bytes, elapsed)
	s.updatePartSizeHintLocked(s.workerPartSizeLocked(workerID))
	s.workerDone[workerID]++
	s.growStartupLocked()
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
	if s.shouldWarmupLocked(workerID) {
		return min(remaining, min(s.maxPartSize, int64(warmupPartSize)))
	}

	if remaining > s.tailWindowLocked() {
		return min(remaining, s.workerPartSizeLocked(workerID))
	}

	targetParts := int64(s.effectiveConcurrencyLocked()) * s.tailPartsPerConnLocked()
	partSize := (remaining + targetParts - 1) / targetParts
	return clampPartSize(partSize, remaining, s.maxPartSize, minTailPartSize)
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

func (s *partScheduler) growStartupLocked() {
	if s.rateLimited || s.maxActive >= s.concurrency {
		return
	}
	s.maxActive++
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
