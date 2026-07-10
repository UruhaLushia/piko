package piko

import "time"

const (
	rateLimitMinActive = 2
	rateLimitCooldown  = 10 * time.Second
	rateLimitRecover   = 15 * time.Second
	rateLimitWindow    = 30 * time.Second
	rateLimitStrikes   = 2
	rateLimitIdle      = 1 * time.Second
)

func (s *partScheduler) limitForRateLimit(delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.normalizeMaxActiveLocked()
	if delay < rateLimitCooldown {
		delay = rateLimitCooldown
	}
	now := time.Now()
	if now.Sub(s.limitedAt) > rateLimitWindow {
		s.limitStrikes = 0
	}
	s.limitedAt = now
	s.limitStrikes++
	if s.limitStrikes >= rateLimitStrikes && s.maxActive > rateLimitMinActive {
		s.maxActive--
		s.clearRateProbeLocked()
		s.limitStrikes = rateLimitStrikes - 1
	}
	s.rateLimited = true
	s.extendRecoveryLocked(now, delay)
}

func (s *partScheduler) recoverRateLimitLocked(now time.Time) {
	s.normalizeMaxActiveLocked()
	if !s.rateLimited || s.maxActive >= s.concurrency || now.Before(s.recoverAt) {
		return
	}
	s.maxActive++
	s.probeLimit = s.maxActive
	s.recoverAt = now.Add(rateLimitRecover)
}

func (s *partScheduler) rateProbeLocked() bool {
	return s.probeLimit == s.maxActive &&
		s.probeLimit > rateLimitMinActive &&
		s.activeCount >= s.probeLimit-1
}

func (s *partScheduler) confirmRateProbe(p part) {
	if !p.rateProbe {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.probeLimit == s.maxActive {
		s.clearRateProbeLocked()
		if s.maxActive >= s.concurrency {
			s.rateLimited = false
		}
	}
}

func (s *partScheduler) rejectRateProbe(delay time.Duration) {
	if delay < rateLimitRecover {
		delay = rateLimitRecover
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.probeLimit == s.maxActive && s.maxActive > rateLimitMinActive {
		s.maxActive--
	}
	s.clearRateProbeLocked()
	s.extendRecoveryLocked(time.Now(), delay)
}

func (s *partScheduler) normalizeMaxActiveLocked() {
	if s.maxActive < 1 || s.maxActive > s.concurrency {
		s.maxActive = s.concurrency
	}
}

func (s *partScheduler) clearRateProbeLocked() {
	s.probeLimit = 0
}

func (s *partScheduler) extendRecoveryLocked(now time.Time, delay time.Duration) {
	recoverAt := now.Add(delay)
	if recoverAt.After(s.recoverAt) {
		s.recoverAt = recoverAt
	}
}
