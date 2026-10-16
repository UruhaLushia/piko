package piko

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const sourceInspectBudget = 1 * time.Second

const sourceMinDeliveredBytes = 8 << 20

type sourceState struct {
	attempts, inflight, failures int
	bytes                        int64
	rate                         float64
	rateAt                       time.Time
	stallRate                    float64
	stallAt                      time.Time
	ttfb                         time.Duration
	headerTimeout                time.Duration
	bodySample                   bool
	probation                    bool
	recoveryBytes                int64
	limit                        int
	limitRecover                 time.Time
	cooldown                     time.Time
	disabled                     bool
	outcome                      string
	finalURL                     string
}

// Keep indexes stable, including rejected sources, so diagnostics retain every URL.
func (d *downloader) inspectSources(ctx context.Context) (remoteInfo, error) {
	if len(d.sources) == 1 {
		info, err := d.inspect(ctx)
		d.sourceState[0].finalURL = info.finalURL
		return info, err
	}

	// Race metadata requests too: the first URL may be unreachable on a
	// different network. Wait for the short probe window and choose the
	// resource signature returned by the largest group of candidates.
	type inspection struct {
		source  int
		info    remoteInfo
		elapsed time.Duration
		err     error
	}
	inspectTimeout := max(DefaultTimeout, d.client.Timeout, d.stallTimeout)
	inspectCtx, cancel := context.WithTimeout(ctx, inspectTimeout)
	defer cancel()
	results := make(chan inspection, len(d.sources))
	for i := range d.sources {
		go func(i int) {
			started := time.Now()
			info, err := d.probeRangeSource(inspectCtx, d.sources[i].URL, i)
			results <- inspection{source: i, info: info, elapsed: time.Since(started), err: err}
		}(i)
	}
	var lastErr error
	valid := make([]inspection, 0, len(d.sources))
	var budget <-chan time.Time
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
collect:
	for range d.sources {
		var result inspection
		select {
		case result = <-results:
		case <-budget:
			break collect
		case <-inspectCtx.Done():
			lastErr = inspectCtx.Err()
			break collect
		}
		s := &d.sourceState[result.source]
		if result.err != nil || !result.info.downloadable() {
			lastErr = result.err
			if errors.Is(result.err, context.DeadlineExceeded) || errors.Is(result.err, context.Canceled) {
				continue
			}
			s.disabled = true
			s.outcome = "rejected"
			continue
		}
		valid = append(valid, result)
		if timer == nil {
			timer = time.NewTimer(sourceInspectBudget)
			budget = timer.C
		}
	}
	cancel()
	if ctx.Err() != nil {
		return remoteInfo{}, ctx.Err()
	}
	if len(valid) == 0 {
		if lastErr == nil {
			lastErr = fmt.Errorf("all download sources rejected")
		}
		return remoteInfo{}, lastErr
	}
	type signature struct {
		size      int64
		rangeable bool
	}
	groups := make(map[signature][]inspection)
	for _, result := range valid {
		key := signature{size: result.info.size, rangeable: result.info.rangeable}
		groups[key] = append(groups[key], result)
	}
	var winners []inspection
	for _, group := range groups {
		if len(group) > len(winners) || (len(group) == len(winners) && len(group) > 0 && group[0].elapsed < winners[0].elapsed) {
			winners = group
		}
	}
	info := winners[0].info
	winner := winners[0].source
	for _, result := range valid {
		s := &d.sourceState[result.source]
		if result.info.size != info.size || result.info.rangeable != info.rangeable {
			s.disabled = true
			s.outcome = "rejected"
			continue
		}
		s.finalURL = result.info.finalURL
		s.ttfb = result.elapsed
	}
	d.primarySource = winner
	d.url = d.urlOf(winner)
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
		probing, measured := 0, false
		for i := range d.sourceState {
			s := &d.sourceState[i]
			totalInflight += s.inflight
			if s.probation {
				probing += s.inflight
			} else if !s.disabled && s.rate > 0 {
				measured = true
			}
		}
		for i := range d.sources {
			s := &d.sourceState[i]
			if s.disabled {
				continue
			}
			available = true
			if s.limit > 0 && s.limit < d.sourceLimit && time.Now().After(s.limitRecover) {
				s.limit++
				s.limitRecover = time.Now().Add(rateLimitRecover)
			}
			limit := d.sourceLimit
			if s.limit > 0 {
				limit = min(limit, s.limit)
			}
			if s.probation {
				limit = 1
				if measured && probing >= max(1, d.sourceConcurrency/4) {
					continue
				}
			}
			if totalInflight >= d.sourceConcurrency || time.Now().Before(s.cooldown) || s.inflight >= limit {
				continue
			}
			eta := s.ttfb.Seconds() + float64(max(bytes, 1))*float64(s.inflight+1)/max(s.rate, 1)
			// Explore unused mirrors before concentrating on measured throughput.
			if s.attempts == 0 || s.probation {
				eta = float64(s.inflight) + s.ttfb.Seconds()
				if s.probation {
					eta += sourceInspectBudget.Seconds()
				}
				if s.finalURL == "" {
					eta += sourceInspectBudget.Seconds()
				}
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

func (d *downloader) recordSourceAttempt(source int) {
	d.sourceMu.Lock()
	d.sourceState[source].attempts++
	d.sourceMu.Unlock()
}

func (d *downloader) finishSource(source int, bytes int64, elapsed time.Duration, err error) {
	d.sourceMu.Lock()
	defer d.sourceMu.Unlock()
	s := &d.sourceState[source]
	s.bytes += max(bytes, 0)
	if _, writeFailed := errors.AsType[rangeWriteError](err); writeFailed {
		return
	}
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errConcurrencyProbeClosed) && !errors.Is(err, errConnectionReplaced) {
		s.recoveryBytes = 0
	}
	if s.rate == 0 && bytes > 0 && elapsed > 0 {
		s.rate = float64(bytes) / (elapsed + s.ttfb).Seconds()
		s.rateAt = time.Now()
	}
	if bytes >= minDynamicPartSize && elapsed >= slowConnectionCheckInterval/4 {
		s.rateAt = time.Now()
		rate := float64(bytes) / elapsed.Seconds()
		if time.Since(s.stallAt) > 2*slowConnectionMinAge {
			s.stallRate = rate
		} else {
			s.stallRate = max(s.stallRate, rate)
		}
		s.stallAt = time.Now()
		if s.rate == 0 {
			s.rate = rate
		} else {
			s.rate = .65*s.rate + .35*rate
		}
	}
	if isSlowConnectionError(err) && bytes < slowConnectionMinBytes && elapsed >= slowConnectionCheckInterval {
		// Small stalled tails must not keep the source's earlier peak as their estimate.
		s.rate = min(s.rate, float64(max(bytes, 0))/elapsed.Seconds())
		s.rateAt = time.Now()
	}
	if err == nil && bytes >= minStealPartSize && bytes < minDynamicPartSize && elapsed+s.ttfb >= slowConnectionCheckInterval/4 {
		s.rate = max(s.rate, float64(bytes)/(elapsed+s.ttfb).Seconds())
		s.rateAt = time.Now()
	}
	if err == nil && bytes > 0 && bytes < minDynamicPartSize && elapsed >= slowConnectionCheckInterval {
		s.rate = min(s.rate, float64(bytes)/elapsed.Seconds())
		s.rateAt = time.Now()
	}
	if errors.Is(err, errConnectionDegraded) {
		d.speedMu.Lock()
		delete(d.recentSpeeds, source)
		d.speedMu.Unlock()
		if s.outcome == "" {
			s.outcome = "used"
		}
		if bytes >= slowConnectionMinBytes {
			s.failures = 0
		} else {
			s.failures++
			if len(d.sources) > 1 {
				s.probation = true
				if s.failures >= 2 {
					s.cooldown = time.Now().Add(time.Second << min(s.failures-2, 5))
					s.outcome = "failed"
				}
			}
		}
		return
	}
	if err == nil && !s.disabled {
		d.recordCompletedSpeed(source, bytes, elapsed)
	}
	if isRateLimitedDownloadError(err) {
		limit := d.sourceLimit
		if s.limit > 0 {
			limit = s.limit
		}
		s.limit = max(1, min(limit-1, s.inflight-1))
		s.limitRecover = time.Now().Add(rateLimitRecover)
		s.outcome = "failed"
		if s.inflight <= 1 {
			s.failures++
			s.cooldown = time.Now().Add(rateLimitDelay(s.failures - 1))
		}
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, errConcurrencyProbeClosed) || errors.Is(err, errConnectionReplaced) {
		return
	}
	if len(d.sources) == 1 {
		s.outcome = "used"
		if err != nil {
			s.outcome = "failed"
		}
		return
	}
	if s.disabled {
		s.outcome = "rejected"
		return
	}
	if err == nil {
		s.outcome = "used"
		if s.probation {
			s.recoveryBytes += bytes
			if s.recoveryBytes < minDynamicPartSize/2 {
				return
			}
		}
		s.failures = 0
		s.probation = false
		s.recoveryBytes = 0
		s.cooldown = time.Time{}
		return
	}
	s.failures++
	s.outcome = "failed"
	s.cooldown = time.Now().Add(time.Second << min(s.failures-1, 5))
	if isSlowConnectionError(err) || errors.Is(err, errResponseHeaderTimeout) || errors.Is(err, errSourceExploration) || isRateProbeTimeout(err) {
		s.probation = true
		return
	}
	truncated := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
	if s.failures >= 2 && (truncated || s.bytes < sourceMinDeliveredBytes) {
		s.disabled = true
		s.outcome = "rejected"
	}
	var status httpStatusError
	var invalid contentRangeError
	if errors.As(err, &invalid) || (errors.As(err, &status) && status.code >= 400 && status.code < 500 && status.code != http.StatusTooManyRequests) {
		s.disabled = true
		s.outcome = "rejected"
	}
	if s.disabled {
		d.speedMu.Lock()
		delete(d.recentSpeeds, source)
		d.speedMu.Unlock()
	}
}

func (d *downloader) claimSourceExploration() bool {
	d.sourceMu.Lock()
	defer d.sourceMu.Unlock()
	if time.Since(d.exploreAt) < slowConnectionCheckInterval {
		return false
	}
	for _, s := range d.sourceState {
		if !s.disabled && s.inflight == 0 && (s.attempts == 0 || (s.probation && time.Now().After(s.cooldown))) {
			d.exploreAt = time.Now()
			return true
		}
	}
	return false
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
		stats[i] = SourceStat{URL: source.URL, Bytes: s.bytes, Attempts: s.attempts, Outcome: outcome, Rate: s.rate, TTFB: s.ttfb}
	}
	return stats
}

func (d *downloader) recordSourceTTFB(source int, elapsed time.Duration) {
	d.sourceMu.Lock()
	s := &d.sourceState[source]
	if !s.bodySample {
		s.ttfb = elapsed
		s.bodySample = true
	} else {
		s.ttfb = (s.ttfb*2 + elapsed) / 3
	}
	d.sourceMu.Unlock()
}

func (d *downloader) responseHeaderBudget(source int, bytes int64) time.Duration {
	d.sourceMu.Lock()
	s := d.sourceState[source]
	d.sourceMu.Unlock()
	if len(d.sources) == 1 {
		return 0
	}
	budget := max(2*slowConnectionCheckInterval, 4*s.ttfb, s.headerTimeout)
	if source == d.primarySource && s.attempts == 0 {
		budget = max(budget, slowConnectionMinAge)
	}
	if !s.bodySample {
		d.speedMu.Lock()
		var rate float64
		peers := 0
		for _, sample := range d.activeSpeeds {
			if sample.rate > 0 && time.Since(sample.at) <= 2*slowConnectionCheckInterval {
				rate += sample.rate
				peers++
			}
		}
		d.speedMu.Unlock()
		if peers > 0 {
			alternative := time.Duration(float64(bytes) / (rate / float64(peers)) * float64(time.Second))
			budget = max(budget, min(slowConnectionMinAge, alternative))
		}
	}
	return budget
}

func (d *downloader) extendResponseHeaderBudget(source int, budget time.Duration) {
	d.sourceMu.Lock()
	s := &d.sourceState[source]
	s.headerTimeout = max(s.headerTimeout, 2*budget)
	d.sourceMu.Unlock()
}
