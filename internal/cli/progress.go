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
	w        io.Writer
	mu       sync.Mutex
	bar      *progressbar.ProgressBar
	total    int64
	finished bool
	latest   piko.Progress
}

func newProgressPrinter(w io.Writer) *progressPrinter {
	return &progressPrinter{w: w}
}

func (p *progressPrinter) Update(progress piko.Progress) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.latest = progress
	p.ensureBarLocked(progress.Total)
	current := progress.Bytes
	if progress.Total > 0 && current > progress.Total {
		current = progress.Total
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
			progressbar.OptionShowBytes(true),
			progressbar.OptionShowTotalBytes(true),
			progressbar.OptionShowCount(),
			progressbar.OptionUseIECUnits(true),
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
