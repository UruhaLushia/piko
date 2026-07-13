package piko

import "time"

func (d *downloader) startStallMonitor(cancel func()) chan<- struct{} {
	if d.stallTimeout <= 0 {
		return nil
	}

	progress := make(chan struct{}, 1)
	go func() {
		timer := time.NewTimer(d.stallTimeout)
		defer timer.Stop()

		for {
			select {
			case _, ok := <-progress:
				if !ok {
					return
				}
				resetTimer(timer, d.stallTimeout)
			case <-timer.C:
				cancel()
				return
			}
		}
	}()
	return progress
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

func (d *downloader) addProgress(n int64, total int64) {
	current := d.done.Add(n)
	if total <= 0 {
		total = d.total
	}
	d.emitProgressWith(current, total, false)
}

func (d *downloader) emitProgress(total int64, done bool) {
	if total <= 0 {
		total = d.total
	}
	d.emitProgressWith(d.done.Load(), total, done)
}

func (d *downloader) emitProgressWith(current int64, total int64, done bool) {
	if d.progress != nil {
		d.progress(Progress{Bytes: current, Total: total, Resumed: d.resumed, Done: done})
	}
}
