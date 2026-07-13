package piko

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

func (d *downloader) downloadPartsToWriter(ctx context.Context, writer io.WriterAt, size int64, partSize int64, concurrency int) error {
	return d.downloadPartsToWriterSpans(ctx, writer, size, partSize, concurrency, nil, 0)
}

func (d *downloader) downloadPartsToWriterSpans(ctx context.Context, writer io.WriterAt, size int64, partSize int64, concurrency int, pending []downloadSpan, completed int64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	d.resumed = completed
	d.total = size
	d.done.Store(completed)
	d.emitProgress(size, false)
	errCh := make(chan error, 1)
	scheduler := newPartScheduler(size, partSize, concurrency, pending)

	var wg sync.WaitGroup
	for workerID := range concurrency {
		client := d.clients[workerID%len(d.clients)]
		wg.Go(func() {
			if err := d.runPartWorker(ctx, scheduler, workerID, client, writer, partSize); err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
			}
		})
	}
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	d.emitProgress(size, true)
	return nil
}

func (d *downloader) runPartWorker(ctx context.Context, scheduler *partScheduler, workerID int, client *http.Client, writer io.WriterAt, partSize int64) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		active, ok := scheduler.nextPart(workerID)
		if !ok {
			if scheduler.hasPendingWork() {
				if err := sleepWithContext(ctx, idlePartPoll); err != nil {
					return nil
				}
				continue
			}
			return nil
		}

		p := active.part
		started := time.Now()
		var confirmProbe func()
		if p.concurrencyProbe {
			confirmProbe = func() {
				scheduler.confirmConcurrencyProbe(workerID, active.probeID)
			}
		}
		offset, err := d.downloadRange(ctx, client, writer, active, p.probeIdleTimeout(), confirmProbe)
		scheduler.finish(workerID, active)
		if err != nil {
			p.end = active.end.Load()
			if offset <= p.end {
				if ctx.Err() == nil && isRetryableDownloadError(err) {
					retry := d.planRangeRetry(scheduler, workerID, p, offset, partSize, err)
					if scheduler.requeue(p, offset, retry.maxRequeues, retry.delay) {
						continue
					}
					err = fmt.Errorf("part %d retry budget exhausted at byte %d: %w", p.index, offset, err)
				}
				return err
			}
		}

		scheduler.confirmRateProbe(p)
		scheduler.record(workerID, max(offset-p.start, 0), time.Since(started))
	}
}
