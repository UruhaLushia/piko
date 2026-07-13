package cli

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/UruhaLushia/piko"
	"github.com/schollz/progressbar/v3"
)

const (
	progressInterval = 500 * time.Millisecond
	progressBarWidth = 28
)

type progressPrinter struct {
	w               io.Writer
	mu              sync.Mutex
	bar             *progressbar.ProgressBar
	total           int64
	finished        bool
	latest          piko.Progress
	started         time.Time
	lastSample      time.Time
	latestAt        time.Time
	lastTransferred int64
	rates           []float64
}

func newProgressPrinter(w io.Writer) *progressPrinter {
	return &progressPrinter{w: w}
}

func (p *progressPrinter) Update(progress piko.Progress) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	if progress.Done && progress.Total > 0 {
		progress.Bytes = progress.Total
	}
	p.latest = progress
	p.latestAt = now
	transferred := max(progress.Bytes-progress.Resumed, 0)
	if p.started.IsZero() && transferred > 0 {
		p.started = now
		p.lastSample = now
		p.lastTransferred = transferred
	}
	updateStats := p.bar == nil || progress.Done
	if !p.started.IsZero() {
		updateStats = p.sampleRateLocked(now, transferred) || updateStats
	}
	p.ensureBarLocked(progress.Total)
	current := progress.Bytes
	if progress.Total > 0 && current > progress.Total {
		current = progress.Total
	}
	if updateStats {
		p.bar.Describe(p.descriptionLocked(now, progress, transferred))
	}
	_ = p.bar.Set64(current)
	if progress.Done {
		p.finished = true
		_ = p.bar.Finish()
	}
}

func (p *progressPrinter) Done() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	p.finished = true
	if p.bar == nil {
		return
	}
	p.ensureBarLocked(p.latest.Total)
	_ = p.bar.Finish()
}

func (p *progressPrinter) Abort() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	p.finished = true
	if p.bar == nil {
		return
	}
	_ = p.bar.Exit()
}

func (p *progressPrinter) Bytes() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.latest.Bytes
}

func (p *progressPrinter) Transferred() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return max(p.latest.Bytes-p.latest.Resumed, 0)
}

func (p *progressPrinter) Elapsed() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started.IsZero() || p.latestAt.Before(p.started) {
		return 0
	}
	return p.latestAt.Sub(p.started)
}

func (p *progressPrinter) sampleRateLocked(now time.Time, transferred int64) bool {
	elapsed := now.Sub(p.lastSample)
	if elapsed < progressInterval {
		return false
	}
	delta := transferred - p.lastTransferred
	if delta >= 0 && elapsed > 0 {
		p.rates = append(p.rates, float64(delta)/elapsed.Seconds())
		if len(p.rates) > 10 {
			p.rates = p.rates[len(p.rates)-10:]
		}
	}
	p.lastSample = now
	p.lastTransferred = transferred
	return true
}

func (p *progressPrinter) descriptionLocked(now time.Time, progress piko.Progress, transferred int64) string {
	elapsed := time.Duration(0)
	if !p.started.IsZero() {
		elapsed = now.Sub(p.started)
	}
	var rate float64
	for _, sample := range p.rates {
		rate += sample
	}
	if len(p.rates) > 0 {
		rate /= float64(len(p.rates))
	} else if transferred > 0 && elapsed > 0 {
		rate = float64(transferred) / elapsed.Seconds()
	}

	eta := "?"
	remaining := max(progress.Total-progress.Bytes, 0)
	if rate > 0 {
		eta = formatProgressDuration(time.Duration(float64(time.Second) * float64(remaining) / rate))
	}
	total := "?"
	if progress.Total > 0 {
		total = formatBytes(progress.Total)
	}
	return fmt.Sprintf("(%s/%s, %s/s) [%s:%s]",
		formatBytes(progress.Bytes),
		total,
		formatBytes(int64(rate)),
		formatProgressDuration(elapsed),
		eta,
	)
}

func formatProgressDuration(duration time.Duration) string {
	if duration < time.Second {
		return "0s"
	}
	return duration.Round(time.Second).String()
}

func (p *progressPrinter) ensureBarLocked(total int64) {
	maxBytes := total
	if maxBytes <= 0 {
		maxBytes = -1
	}
	if p.bar == nil {
		p.total = maxBytes
		p.bar = progressbar.NewOptions64(
			maxBytes,
			progressbar.OptionSetWriter(p.w),
			progressbar.OptionSetWidth(progressBarWidth),
			progressbar.OptionSetTheme(progressbar.ThemeASCII),
			progressbar.OptionSetRenderBlankState(true),
			progressbar.OptionUseANSICodes(true),
			progressbar.OptionSetPredictTime(false),
			progressbar.OptionSetElapsedTime(false),
			progressbar.OptionShowDescriptionAtLineEnd(),
			progressbar.OptionThrottle(progressInterval),
			progressbar.OptionOnCompletion(func() {
				fmt.Fprintln(p.w)
			}),
			progressbar.OptionSpinnerType(14),
		)
		return
	}

	if maxBytes > 0 && p.total != maxBytes {
		p.total = maxBytes
		p.bar.ChangeMax64(maxBytes)
	}
}
