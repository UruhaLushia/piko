package dialer

import (
	"errors"
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
)

var ErrSlowConnection = errors.New("slow connection")

type ipQuality struct {
	emaBps           float64
	samples          int
	slowStreak       int
	failureStreak    int
	quarantinedUntil time.Time
}

func (s *Selector) RecordIP(key string, bytes int64, elapsed time.Duration, err error) {
	if s == nil || key == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stats == nil {
		s.stats = make(map[string]*ipQuality)
	}
	stat := s.stats[key]
	if stat == nil {
		stat = &ipQuality{}
		s.stats[key] = stat
	}

	if err != nil && bytes <= 0 {
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
	}
	if stat.slowStreak >= ipQualitySlowThreshold {
		stat.quarantinedUntil = time.Now().Add(ipQualityQuarantineDuration)
	}
}

func (s *Selector) averageIPSpeedLocked(exclude string) float64 {
	var total float64
	var count int
	for key, stat := range s.stats {
		if key == exclude || stat.samples == 0 || stat.emaBps <= 0 {
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
		if stat != nil {
			return stat.emaBps
		}
		return 0
	}
	return total / float64(count)
}
