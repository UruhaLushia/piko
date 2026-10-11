package piko

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const sourceInspectBudget = 1 * time.Second

type sourceState struct {
	attempts, inflight, failures int
	bytes                        int64
	rate                         float64
	ttfb                         time.Duration
	cooldown                     time.Time
	disabled                     bool
	outcome                      string
	finalURL                     string
}

// Keep indexes stable, including rejected sources, so diagnostics retain every URL.
func (d *downloader) inspectSources(ctx context.Context) (remoteInfo, error) {
	var info remoteInfo
	var err error
	for i := range d.sources {
		d.primarySource = i
		d.url = d.sources[i].URL
		info, err = d.inspect(ctx)
		if ctx.Err() != nil {
			return remoteInfo{}, ctx.Err()
		}
		if err == nil && info.downloadable() {
			break
		}
		d.sourceState[i].disabled = true
		d.sourceState[i].outcome = "rejected"
		if err == nil {
			err = fmt.Errorf("source %d has no downloadable resource", i)
		}
	}
	if err != nil {
		return remoteInfo{}, err
	}
	d.sourceState[d.primarySource].finalURL = info.finalURL
	if !info.hasSizedRange() {
		return info, nil
	}
	inspectCtx, cancel := context.WithTimeout(ctx, sourceInspectBudget)
	defer cancel()
	var wg sync.WaitGroup
	for i := range d.sources {
		if i == d.primarySource || d.sourceState[i].disabled {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			candidate, probeErr := d.probeRangeSource(inspectCtx, d.sources[i].URL, i)
			if probeErr != nil || !candidate.hasSizedRange() || candidate.size != info.size {
				if errors.Is(probeErr, context.DeadlineExceeded) || errors.Is(probeErr, context.Canceled) {
					return
				}
				d.sourceState[i].disabled = true
				d.sourceState[i].outcome = "rejected"
				return
			}
			d.sourceState[i].finalURL = candidate.finalURL
		}(i)
	}
	wg.Wait()
	return info, nil
}

// Selection and reservation share a lock: concurrent workers cannot overbook a source.
func (d *downloader) acquireSource(ctx context.Context, bytes int64) (int, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if d.hedgeMismatch.Load() {
			return 0, ErrSourceMismatch
		}
		d.sourceMu.Lock()
		best, score, available := -1, 0.0, false
		totalInflight := 0
		for i := range d.sourceState {
			totalInflight += d.sourceState[i].inflight
		}
		for i := range d.sources {
			s := &d.sourceState[i]
			if s.disabled {
				continue
			}
			available = true
			if totalInflight >= d.sourceConcurrency || time.Now().Before(s.cooldown) || s.inflight >= d.sourceLimit {
				continue
			}
			eta := s.ttfb.Seconds() + float64(max(bytes, 1))*float64(s.inflight+1)/max(s.rate, 1)
			// Explore unused mirrors before concentrating on measured throughput.
			if s.attempts == 0 {
				eta = float64(s.inflight)
			}
			if best < 0 || eta < score {
				best, score = i, eta
			}
		}
		if best >= 0 {
			d.sourceState[best].inflight++
		}
		d.sourceMu.Unlock()
		if best >= 0 {
			return best, nil
		}
		if !available {
			return 0, fmt.Errorf("all download sources rejected")
		}
		if err := sleepWithContext(ctx, idlePartPoll); err != nil {
			return 0, err
		}
	}
}

func (d *downloader) releaseSource(source int) {
	d.sourceMu.Lock()
	d.sourceState[source].inflight--
	d.sourceMu.Unlock()
}

func (d *downloader) recordSourceAttempt(source int, _ int64) {
	d.sourceMu.Lock()
	d.sourceState[source].attempts++
	d.sourceMu.Unlock()
}

func (d *downloader) finishSource(source int, bytes int64, elapsed time.Duration, err error) {
	d.sourceMu.Lock()
	defer d.sourceMu.Unlock()
	s := &d.sourceState[source]
	s.bytes += max(bytes, 0)
	if bytes >= 16*1024 && elapsed > 0 {
		rate := float64(bytes) / elapsed.Seconds()
		if s.rate == 0 {
			s.rate = rate
		} else {
			s.rate = .65*s.rate + .35*rate
		}
	}
	if len(d.sources) == 1 {
		s.outcome = "used"
		if err != nil {
			s.outcome = "failed"
		}
		return
	}
	if err == nil {
		s.failures = 0
		s.cooldown = time.Time{}
		s.outcome = "used"
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, errConcurrencyProbeClosed) {
		return
	}
	s.failures++
	s.outcome = "failed"
	s.cooldown = time.Now().Add(time.Second << min(s.failures-1, 5))
	// A multi-source download must stop spending retries on a mirror that has
	// already demonstrated a slow or broken range. Keep transient HTTP errors
	// retryable, but remove the bad source from subsequent range scheduling.
	if len(d.sources) > 1 && (isSlowConnectionError(err) || s.failures >= 2) {
		s.disabled = true
		s.outcome = "rejected"
	}
	var status httpStatusError
	var invalid contentRangeError
	if errors.As(err, &invalid) || (errors.As(err, &status) && status.code >= 400 && status.code < 500 && status.code != http.StatusTooManyRequests) {
		s.disabled = true
		s.outcome = "rejected"
	}
}

func (d *downloader) urlOf(source int) string {
	if u := d.sourceState[source].finalURL; u != "" {
		return u
	}
	return d.sources[source].URL
}

func (d *downloader) sourceStats() []SourceStat {
	d.sourceMu.Lock()
	defer d.sourceMu.Unlock()
	stats := make([]SourceStat, len(d.sources))
	for i, source := range d.sources {
		s := d.sourceState[i]
		outcome := s.outcome
		if outcome == "" {
			outcome = "unused"
		}
		stats[i] = SourceStat{URL: source.URL, Bytes: s.bytes, Attempts: s.attempts, Outcome: outcome}
	}
	return stats
}

func (d *downloader) recordSourceHeaders(source int, elapsed time.Duration) {
	d.sourceMu.Lock()
	s := &d.sourceState[source]
	if s.ttfb == 0 {
		s.ttfb = elapsed
	} else {
		s.ttfb = (s.ttfb*2 + elapsed) / 3
	}
	d.sourceMu.Unlock()
}
