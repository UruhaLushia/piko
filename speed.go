package piko

import (
	"context"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	"github.com/UruhaLushia/piko/internal/dialer"
)

const (
	slowConnectionCheckInterval = time.Second
	slowConnectionMinAge        = 5 * time.Second
	slowConnectionMinBytes      = 1024 * 1024
	slowConnectionRatio         = 0.45
	slowConnectionSourceRatio   = 0.6
	slowConnectionStrikes       = 2
	slowConnectionMinPeers      = 2
	slowTailWindow              = 2 * 1024 * 1024
	rangeSpeedSourceSlow        = 1
	rangeSpeedConnectionSlow    = 2
	rangeSpeedExplore           = 3
)

func (d *downloader) registerRangeSpeed(source int) int64 {
	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	d.nextSpeedID++
	id := d.nextSpeedID
	if d.activeSpeeds == nil {
		d.activeSpeeds = make(map[int64]rangeSpeed)
	}
	d.activeSpeeds[id] = rangeSpeed{source: source}
	return id
}

func (d *downloader) unregisterRangeSpeed(id int64) {
	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	delete(d.activeSpeeds, id)
}

func (d *downloader) updateRangeSpeed(id int64, speed float64, window time.Duration) (float64, int, float64) {
	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	if d.activeSpeeds == nil {
		return 0, 0, 0
	}
	current := d.activeSpeeds[id]
	current.rate = speed
	current.at = time.Now()
	d.activeSpeeds[id] = current

	var total float64
	peers := 0
	var otherTotal float64
	otherPeers := 0
	seen := make(map[int]bool)
	for _, active := range d.activeSpeeds {
		if time.Since(active.at) > 2*slowConnectionCheckInterval {
			continue
		}
		seen[active.source] = true
		if active.rate <= 0 {
			continue
		}
		total += active.rate
		peers++
		if active.source != current.source {
			otherTotal += active.rate
			otherPeers++
		}
	}
	for source, recent := range d.recentSpeeds {
		if source != current.source && !seen[source] && time.Since(recent.at) <= window {
			otherTotal += recent.rate
			otherPeers++
		}
	}
	otherAvg := 0.0
	if otherPeers > 0 {
		otherAvg = otherTotal / float64(otherPeers)
	}
	return total / float64(max(peers, 1)), peers, otherAvg
}

func (d *downloader) recordCompletedSpeed(source int, bytes int64, elapsed time.Duration) {
	if bytes < slowConnectionMinBytes || elapsed < slowConnectionCheckInterval/4 {
		return
	}
	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	if d.recentSpeeds == nil {
		d.recentSpeeds = make(map[int]rangeSpeed)
	}
	d.recentSpeeds[source] = rangeSpeed{source: source, rate: float64(bytes) / elapsed.Seconds(), at: time.Now()}
}

type rangeSpeedMonitor struct {
	reason atomic.Int32
}

func (m *rangeSpeedMonitor) err() error {
	switch m.reason.Load() {
	case rangeSpeedSourceSlow:
		return dialer.ErrSlowConnection
	case rangeSpeedConnectionSlow:
		return errConnectionDegraded
	case rangeSpeedExplore:
		return errSourceExploration
	default:
		return nil
	}
}

func (d *downloader) recentSourceRate(source int, window time.Duration) float64 {
	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	sample := d.recentSpeeds[source]
	if time.Since(sample.at) > window {
		return 0
	}
	return sample.rate
}

func (d *downloader) completedRangeSlowed(source int, bytes int64, elapsed time.Duration) bool {
	if bytes <= 0 || elapsed < slowConnectionCheckInterval {
		return false
	}
	reference := d.recentSourceRate(source, 2*slowConnectionMinAge)
	return reference > 0 && float64(bytes)/elapsed.Seconds() < reference*slowConnectionRatio
}

func (d *downloader) startRangeSpeedMonitor(ctx context.Context, active *activePart, start int64, connection httptrace.GotConnInfo, ipKey string, abort func()) (func(), *rangeSpeedMonitor) {
	expired := &rangeSpeedMonitor{}
	source := active.part.source
	id := d.registerRangeSpeed(source)
	d.sourceMu.Lock()
	state := d.sourceState[source]
	d.sourceMu.Unlock()
	ttfb := state.ttfb
	var ipRate float64
	if ipKey != "" {
		var ipTTFB time.Duration
		ipRate, ipTTFB = d.selector.Reference(ipKey)
		if !state.bodySample {
			ttfb = max(ttfb, ipTTFB)
		}
	}
	minAge := max(slowConnectionMinAge, 4*ttfb)
	peakRate := d.recentSourceRate(source, 2*minAge)
	stallRate := 0.0
	if time.Since(state.stallAt) <= 2*minAge {
		stallRate = state.stallRate
	}
	if time.Since(state.rateAt) <= 2*minAge {
		peakRate = max(peakRate, state.rate)
	}
	if state.bodySample && (state.probation || peakRate > 0) {
		minAge = max(slowConnectionCheckInterval, 4*ttfb)
	}
	if connection.Reused {
		if peakRate == 0 {
			peakRate = ipRate
		}
		if peakRate > 0 && state.bodySample {
			// A reused connection with recent throughput has already passed warmup.
			minAge = max(slowConnectionCheckInterval, 4*ttfb)
		}
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		defer d.unregisterRangeSpeed(id)
		ticker := time.NewTicker(slowConnectionCheckInterval)
		defer ticker.Stop()
		started := time.Now()
		stallAge := max(slowConnectionCheckInterval, 4*ttfb)
		if !state.bodySample {
			stallAge = max(stallAge, minAge)
		}
		lastCheck, lastOffset, strikes := started, start, 0
		lastWriteNanos := active.writeNanos.Load()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case now := <-ticker.C:
				offset := active.offset.Load()
				if offset-start >= slowConnectionMinBytes {
					minAge = max(slowConnectionCheckInterval, 4*ttfb)
					stallAge = minAge
				}
				writeNanos := active.writeNanos.Load()
				writeTime := time.Duration(writeNanos - lastWriteNanos)
				lastWriteNanos = writeNanos
				if active.writing.Load() || writeTime >= now.Sub(lastCheck)/4 {
					lastCheck, lastOffset, strikes = now, offset, 0
					continue
				}
				remaining := active.end.Load() - offset + 1
				if remaining <= 0 {
					return
				}
				active.mu.Lock()
				windowStart := lastCheck
				if active.bodyStarted.After(windowStart) && active.bodyStarted.Before(now) {
					windowStart = active.bodyStarted
				}
				speed := float64(offset-lastOffset) / now.Sub(windowStart).Seconds()
				active.recentRate, active.recentRateAt = speed, now
				allowSlow := active.part.allowSlow
				active.mu.Unlock()
				avg, peers, otherAvg := d.updateRangeSpeed(id, speed, 2*minAge)
				connectionSlow := peakRate > 0 && speed < peakRate*slowConnectionRatio
				stalled := speed == 0 && max(stallRate, peakRate, otherAvg)*now.Sub(lastCheck).Seconds() >= slowConnectionMinBytes
				switch {
				case stalled && now.Sub(started) >= stallAge:
					strikes = slowConnectionStrikes
				case !allowSlow && now.Sub(started) >= minAge && (connectionSlow || shouldCloseSlowConnection(speed, avg, otherAvg, peers, offset-start)):
					strikes++
				default:
					strikes = 0
				}
				lastCheck, lastOffset = now, offset
				peakRate = max(peakRate, speed)
				if strikes < slowConnectionStrikes && now.Sub(started) >= minAge && (speed == 0 || float64(remaining)/speed > minAge.Seconds()) && d.claimSourceExploration() {
					expired.reason.Store(rangeSpeedExplore)
					abort()
					return
				}
				if strikes >= slowConnectionStrikes {
					reason := int32(rangeSpeedSourceSlow)
					if connectionSlow {
						reason = rangeSpeedConnectionSlow
					}
					expired.reason.Store(reason)
					abort()
					return
				}
			}
		}
	}()
	return func() { close(stop); <-done }, expired
}
