package piko

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync"
	"time"
)

type part struct {
	index    int
	start    int64
	end      int64
	requeues int
}

type rangeConnection struct {
	conn net.Conn
	key  string
}

func (p part) length() int64 {
	return p.end - p.start + 1
}

func (d *downloader) downloadParts(ctx context.Context, output string, size int64, partSize int64, concurrency int, force bool) error {
	discard := IsNullOutput(output)
	partPath := ""
	var writer io.WriterAt = discardWriterAt{}
	var file *os.File
	if !discard {
		partPath = output + ".part"
		if err := prepareTemp(partPath); err != nil {
			return err
		}

		var err error
		file, err = os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		if err := file.Truncate(size); err != nil {
			file.Close()
			_ = os.Remove(partPath)
			return err
		}
		writer = file
	}

	err := d.downloadPartsToWriter(ctx, writer, size, partSize, concurrency)
	if closeErr := closeFile(file); err == nil {
		err = closeErr
	}
	if err != nil {
		if !discard {
			_ = os.Remove(partPath)
		}
		return err
	}

	if discard {
		return nil
	}
	return finishOutput(partPath, output, force)
}

func (d *downloader) downloadPartsToWriter(ctx context.Context, writer io.WriterAt, size int64, partSize int64, concurrency int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	d.done.Store(0)
	d.emitProgress(size, false)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	scheduler := newPartScheduler(size, partSize, concurrency)

	startWorkers := make(chan struct{})
	for workerID := range concurrency {
		client := d.clients[workerID%len(d.clients)]
		wg.Go(func() {
			select {
			case <-startWorkers:
			case <-ctx.Done():
				return
			}
			for {
				if err := ctx.Err(); err != nil {
					return
				}
				p, ok := scheduler.nextPart(workerID)
				if !ok {
					if scheduler.hasInFlight() {
						if err := sleepWithContext(ctx, idlePartPoll); err != nil {
							return
						}
						continue
					}
					return
				}
				active := scheduler.activate(workerID, p)
				started := time.Now()
				offset, err := d.downloadRange(ctx, client, writer, active)
				scheduler.finish(workerID, active)
				if err != nil {
					p.end = active.end.Load()
					if ctx.Err() == nil && isRetryableDownloadError(err) {
						rateLimited := isRateLimitedDownloadError(err)
						maxRequeues := max(d.retries*4, 8)
						delay := time.Duration(0)
						if rateLimited {
							maxRequeues = max(d.retries*16, 64)
							delay = rateLimitDelay(p.requeues)
							if p.end-offset+1 <= max(partSize, int64(minDynamicPartSize*2)) {
								delay = 0
							}
						} else {
							scheduler.penalize(workerID)
						}
						if scheduler.requeue(p, offset, maxRequeues, delay) {
							continue
						}
						err = fmt.Errorf("part %d retry budget exhausted at byte %d: %w", p.index, offset, err)
					}
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
				scheduler.record(workerID, max(offset-p.start, 0), time.Since(started))
			}
		})
	}
	close(startWorkers)
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

func (d *downloader) downloadRange(ctx context.Context, client *http.Client, writer io.WriterAt, active *activePart) (int64, error) {
	p := active.part
	offset := p.start
	var lastErr error
	for attempt := 0; attempt <= d.retries; attempt++ {
		end := active.end.Load()
		if offset > end {
			return offset, nil
		}
		if err := ctx.Err(); err != nil {
			return offset, err
		}
		active.offset.Store(offset)

		attemptCtx, attemptCancel := context.WithCancel(ctx)
		cancelID := active.setCancel(attemptCancel)
		connInfo := &rangeConnection{}
		attemptCtx = httptrace.WithClientTrace(attemptCtx, &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				connInfo.conn = info.Conn
				if info.Conn != nil {
					connInfo.key = remoteAddrIPKey(info.Conn.RemoteAddr())
				}
			},
		})
		finishAttempt := func() {
			attemptCancel()
			active.clearCancel(cancelID)
		}
		req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, d.url, nil)
		if err != nil {
			finishAttempt()
			return offset, err
		}
		d.setCommonHeaders(req)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))

		attemptStart := offset
		attemptStarted := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			if resp != nil {
				resp.Body.Close()
			}
			d.recordIPAttempt(connInfo.key, offset-attemptStart, time.Since(attemptStarted), err)
			finishAttempt()
			if offset > active.end.Load() {
				return offset, nil
			}
			lastErr = err
			if !isRetryableDownloadError(err) {
				return offset, err
			}
			if ctx.Err() == nil && attemptCtx.Err() != nil {
				return offset, err
			}
		} else {
			err = d.copyRange(attemptCtx, attemptCancel, writer, resp, p.index, attemptStart, end, &offset, active, partLease(p), connInfo.conn)
			resp.Body.Close()
			d.recordIPAttempt(connInfo.key, offset-attemptStart, time.Since(attemptStarted), err)
			finishAttempt()
			if err == nil {
				return offset, nil
			}
			if offset > active.end.Load() {
				return offset, nil
			}
			lastErr = err
			if !isRetryableDownloadError(err) {
				return offset, err
			}
			if isRateLimitedDownloadError(err) {
				return offset, err
			}
			if ctx.Err() == nil && (offset > attemptStart || attemptCtx.Err() != nil) {
				return offset, err
			}
		}

		if attempt < d.retries {
			if err := sleepWithContext(ctx, retryDelay(attempt)); err != nil {
				return offset, err
			}
		}
	}
	return offset, fmt.Errorf("part %d failed at byte %d: %w", p.index, offset, lastErr)
}

func (d *downloader) copyRange(ctx context.Context, cancel context.CancelFunc, writer io.WriterAt, resp *http.Response, partIndex int, requestStart int64, requestEnd int64, offset *int64, active *activePart, lease time.Duration, conn net.Conn) error {
	if resp.StatusCode != http.StatusPartialContent {
		return httpStatusError{partIndex: partIndex, code: resp.StatusCode, status: resp.Status}
	}
	if err := validateContentRange(partIndex, resp.Header.Get("Content-Range"), requestStart, requestEnd); err != nil {
		return err
	}

	buf := make([]byte, copyBufferSize)
	progress := d.startStallMonitor(cancel)
	if progress != nil {
		defer close(progress)
	}
	stopLease := startLeaseMonitor(cancel, lease)
	defer stopLease()

	speedID := d.registerRangeSpeed()
	defer d.unregisterRangeSpeed(speedID)
	started := time.Now()
	lastCheck := started
	lastOffset := *offset
	slowStrikes := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		end := active.end.Load()
		if *offset > end {
			return nil
		}
		readSize := min(int64(len(buf)), end-*offset+1)
		n, readErr := resp.Body.Read(buf[:int(readSize)])
		if n > 0 {
			active.mu.Lock()
			end = active.end.Load()
			if *offset > end {
				active.mu.Unlock()
				return nil
			}
			writeSize := min(int64(n), end-*offset+1)
			written, writeErr := writer.WriteAt(buf[:int(writeSize)], *offset)
			if writeErr != nil {
				active.mu.Unlock()
				return writeErr
			}
			if int64(written) != writeSize {
				active.mu.Unlock()
				return io.ErrShortWrite
			}
			*offset += writeSize
			active.offset.Store(*offset)
			active.mu.Unlock()
			if writeSize > 0 {
				d.addProgress(writeSize, 0)
				now := time.Now()
				if now.Sub(lastCheck) >= slowConnectionCheckInterval {
					speed := float64(*offset-lastOffset) / now.Sub(lastCheck).Seconds()
					avg, peers := d.updateRangeSpeed(speedID, speed)
					if shouldCloseSlowConnection(speed, avg, peers, now.Sub(started), *offset-requestStart) {
						slowStrikes++
					} else {
						slowStrikes = 0
					}
					lastCheck = now
					lastOffset = *offset
					if slowStrikes >= slowConnectionStrikes {
						if conn != nil {
							_ = conn.Close()
						}
						cancel()
						return errSlowConnection
					}
				}
			}
			if progress != nil {
				select {
				case progress <- struct{}{}:
				default:
				}
			}
			if writeSize < int64(n) {
				return nil
			}
		}

		if readErr == io.EOF {
			if *offset > active.end.Load() {
				return nil
			}
			return io.ErrUnexpectedEOF
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (d *downloader) recordIPAttempt(key string, bytes int64, elapsed time.Duration, err error) {
	if d.selector != nil {
		d.selector.recordIP(key, bytes, elapsed, err)
	}
}

func shouldCloseSlowConnection(speed float64, avg float64, peers int, age time.Duration, bytes int64) bool {
	return peers >= slowConnectionMinPeers &&
		age >= slowConnectionMinAge &&
		bytes >= slowConnectionMinBytes &&
		avg > 0 &&
		speed > 0 &&
		speed < avg*slowConnectionRatio
}

type discardWriterAt struct{}

func (discardWriterAt) WriteAt(p []byte, _ int64) (int, error) {
	return len(p), nil
}
