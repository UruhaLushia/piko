package dialer

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	ipQualitySmoothFactor       = 0.35
	ipQualityUnknownWeight      = 4
	ipQualityMaxWeight          = 12
	ipQualityMinSampleBytes     = 256 * 1024
	ipQualityMinSampleDuration  = 300 * time.Millisecond
	ipQualitySlowRatio          = 0.55
	ipQualitySlowThreshold      = 3
	ipQualityFailureThreshold   = 3
	ipQualityQuarantineDuration = 45 * time.Second
	ipQualityLifetime           = 5 * time.Minute
)

var (
	ErrSlowConnection     = errors.New("slow connection")
	ErrConnectionDegraded = fmt.Errorf("connection throughput dropped: %w", ErrSlowConnection)
)

type ipQuality struct {
	emaBps           float64
	samples          int
	slowStreak       int
	failureStreak    int
	quarantinedUntil time.Time
	ttfb             time.Duration
	updatedAt        time.Time
	needsRecovery    bool
}

func (s *Selector) RecordIP(key string, bytes int64, elapsed, ttfb time.Duration, err error) {
	if s == nil || key == "" || errors.Is(err, context.Canceled) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stats == nil {
		s.stats = make(map[string]*ipQuality)
	}
	stat := s.stats[key]
	if stat == nil {
		for address, entry := range s.stats {
			if time.Since(entry.updatedAt) > ipQualityLifetime {
				delete(s.stats, address)
			}
		}
	}
	if stat == nil || time.Since(stat.updatedAt) > ipQualityLifetime {
		stat = &ipQuality{updatedAt: time.Now()}
		s.stats[key] = stat
	}
	if ttfb > 0 {
		if stat.ttfb == 0 {
			stat.ttfb = ttfb
		} else {
			stat.ttfb = (stat.ttfb*2 + ttfb) / 3
		}
	}
	if errors.Is(err, ErrConnectionDegraded) {
		// A new TCP connection may recover on this IP; retire only its old speed reference.
		stat.needsRecovery = true
		stat.updatedAt = time.Now()
		return
	}

	if err != nil && bytes <= 0 {
		stat.updatedAt = time.Now()
		stat.failureStreak++
		if stat.failureStreak >= ipQualityFailureThreshold {
			stat.quarantinedUntil = time.Now().Add(ipQualityQuarantineDuration)
		}
		return
	}
	if elapsed < ipQualityMinSampleDuration || bytes < ipQualityMinSampleBytes {
		return
	}

	speed := float64(bytes) / elapsed.Seconds()
	if speed <= 0 {
		return
	}
	stat.updatedAt = time.Now()
	if stat.emaBps <= 0 {
		stat.emaBps = speed
	} else {
		stat.emaBps = stat.emaBps*(1-ipQualitySmoothFactor) + speed*ipQualitySmoothFactor
	}
	stat.samples++

	avg := s.averageIPSpeedLocked(key)
	if errors.Is(err, ErrSlowConnection) || (avg > 0 && speed < avg*ipQualitySlowRatio) {
		stat.slowStreak++
	} else {
		stat.slowStreak = 0
		stat.failureStreak = 0
		stat.quarantinedUntil = time.Time{}
		stat.needsRecovery = false
	}
	if stat.slowStreak >= ipQualitySlowThreshold {
		stat.quarantinedUntil = time.Now().Add(ipQualityQuarantineDuration)
	}
}

func (s *Selector) Reference(key string) (float64, time.Duration) {
	if s == nil || key == "" {
		return 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stat := s.stats[key]
	if stat == nil || time.Since(stat.updatedAt) > ipQualityLifetime {
		return 0, 0
	}
	if stat.samples < 2 || stat.failureStreak > 0 || stat.slowStreak > 0 || stat.needsRecovery || time.Now().Before(stat.quarantinedUntil) {
		return 0, stat.ttfb
	}
	return stat.emaBps, stat.ttfb
}

func (s *Selector) averageIPSpeedLocked(exclude string) float64 {
	var total float64
	var count int
	for key, stat := range s.stats {
		if key == exclude || stat.samples == 0 || stat.emaBps <= 0 || time.Since(stat.updatedAt) > ipQualityLifetime {
			continue
		}
		total += stat.emaBps
		count++
	}
	if count == 0 {
		if exclude == "" {
			return 0
		}
		stat := s.stats[exclude]
		if stat != nil && time.Since(stat.updatedAt) <= ipQualityLifetime {
			return stat.emaBps
		}
		return 0
	}
	return total / float64(count)
}
