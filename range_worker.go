package piko

import (
	"context"
	"errors"
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
	scheduler := newPartScheduler(size, partSize, concurrency, pending, d.concurrencyStrategy)

	var wg sync.WaitGroup
	for workerID := range concurrency {
		client := d.clients[workerID%len(d.clients)]
		wg.Go(func() {
			if err := d.runPartWorker(ctx, scheduler, workerID, client, writer); err != nil {
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

func (d *downloader) runPartWorker(ctx context.Context, scheduler *partScheduler, workerID int, client *http.Client, writer io.WriterAt) error {
	var buffers rangeBuffers
	for {
		if ctx.Err() != nil {
			return nil
		}
		active, ok := scheduler.nextPart(workerID)
		if !ok {
			if !scheduler.workerEnabled(workerID) {
				client.CloseIdleConnections()
			}
			if scheduler.hasPendingWork() {
				if err := sleepWithContext(ctx, idlePartPoll); err != nil {
					return nil
				}
				continue
			}
			return nil
		}
		active.connMu.Lock()
		active.retire = d.retireConnection
		active.connMu.Unlock()
		source, err := d.acquireSource(ctx, active.part.length())
		if err != nil {
			scheduler.finish(workerID, active)
			return err
		}

		d.sourceMu.Lock()
		state := d.sourceState[source]
		startup := max(state.ttfb, state.headerTimeout/2)
		if !state.bodySample {
			startup = max(startup, slowConnectionMinAge)
		}
		d.sourceMu.Unlock()
		if state.bodySample && !state.probation && time.Since(state.rateAt) <= 2*slowConnectionMinAge {
			scheduler.prepare(workerID, active, state.rate, state.ttfb)
		}
		p := active.part
		p.source = source
		active.mu.Lock()
		active.part.source = source
		active.started = time.Now()
		active.startup = startup
		active.ready = true
		active.mu.Unlock()
		var probeProgress func(int64)
		if p.concurrencyProbe {
			probeProgress = func(bytes int64) {
				scheduler.recordConcurrencyProbe(workerID, bytes)
			}
		}
		offset, err := d.downloadRange(ctx, client, writer, active, source, &buffers, p.probeIdleTimeout(), probeProgress)
		d.releaseSource(p.source)
		scheduler.finish(workerID, active)
		if err != nil {
			if _, writeFailed := errors.AsType[rangeWriteError](err); writeFailed {
				return err
			}
			if errors.Is(err, ErrSourceMismatch) {
				return err
			}
			p.end = active.end.Load()
			if offset <= p.end {
				if ctx.Err() == nil && (len(d.sources) > 1 || isRetryableDownloadError(err)) {
					if errors.Is(err, errConnectionDegraded) && offset-p.start >= slowConnectionMinBytes {
						p.requeues = 0
					}
					retry := d.planRangeRetry(scheduler, workerID, p, err)
					if isSlowConnectionError(err) && !errors.Is(err, errConnectionDegraded) {
						p.slowRetries++
						p.allowSlow = p.slowRetries >= slowConnectionRetryLimit
					}
					if scheduler.requeue(p, offset, retry.maxRequeues, retry.delay) {
						continue
					}
					err = fmt.Errorf("part %d retry budget exhausted at byte %d: %w", p.index, offset, err)
				}
				return err
			}
		}

		scheduler.confirmRateProbe(p)
		active.mu.Lock()
		bodyStarted, bodyOffset := active.bodyStarted, active.bodyOffset
		writeTime := time.Duration(active.writeNanos.Load() - active.bodyWriteNanos)
		latency := bodyStarted.Sub(active.started)
		active.mu.Unlock()
		if !bodyStarted.IsZero() {
			scheduler.record(workerID, max(offset-bodyOffset, 0), max(time.Since(bodyStarted)-writeTime, time.Nanosecond), latency)
		}
	}
}
