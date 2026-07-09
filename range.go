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
	var asyncWriter *asyncFileWriterAt
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
		asyncWriter = newAsyncFileWriterAt(file)
		writer = asyncWriter
	}

	err := d.downloadPartsToWriter(ctx, writer, size, partSize, concurrency)
	if asyncWriter != nil {
		if writeErr := asyncWriter.Close(); err == nil {
			err = writeErr
		}
	}
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
			closeRange := shouldCloseRangeConnection(err, offset, end, active.end.Load())
			if closeRange {
				attemptCancel()
				if connInfo.conn != nil {
					_ = connInfo.conn.Close()
				}
			}
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

func shouldCloseRangeConnection(err error, offset int64, requestEnd int64, activeEnd int64) bool {
	if err != nil {
		return true
	}
	return activeEnd < requestEnd || offset <= requestEnd
}

func (d *downloader) copyRange(ctx context.Context, cancel context.CancelFunc, writer io.WriterAt, resp *http.Response, partIndex int, requestStart int64, requestEnd int64, offset *int64, active *activePart, lease time.Duration, conn net.Conn) error {
	if resp.StatusCode != http.StatusPartialContent {
		return httpStatusError{partIndex: partIndex, code: resp.StatusCode, status: resp.Status}
	}
	if err := validateContentRange(partIndex, resp.Header.Get("Content-Range"), requestStart, requestEnd); err != nil {
		return err
	}

	buffered := shouldBufferRangeWrite(writer, requestEnd-requestStart+1)
	return d.copyRangeBody(ctx, cancel, writer, resp.Body, requestStart, offset, active, lease, conn, buffered)
}

func shouldBufferRangeWrite(writer io.WriterAt, size int64) bool {
	if size > maxBufferedRangeSize {
		return false
	}
	switch writer.(type) {
	case discardWriterAt, byteSliceWriterAt:
		return false
	default:
		return true
	}
}

func (d *downloader) copyRangeBody(ctx context.Context, cancel context.CancelFunc, writer io.WriterAt, reader io.Reader, requestStart int64, offset *int64, active *activePart, lease time.Duration, conn net.Conn, buffered bool) error {
	state := rangeWriteState{
		d:        d,
		writer:   writer,
		active:   active,
		offset:   offset,
		buffered: buffered,
	}
	if buffered {
		state.buffer = make([]byte, 0, int(min(active.part.length(), int64(maxBufferedRangeSize))))
	}
	buf := make([]byte, rangeWriteBufferSize)
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
			return state.finish(err)
		}

		end := active.end.Load()
		if *offset > end {
			return state.flush()
		}
		readSize := min(int64(len(buf)), end-*offset+1)
		n, readErr := reader.Read(buf[:int(readSize)])
		if n > 0 {
			if progress != nil {
				select {
				case progress <- struct{}{}:
				default:
				}
			}
			writeSize, writeErr := state.write(buf[:n])
			if writeErr != nil {
				return writeErr
			}
			if writeSize > 0 {
				now := time.Now()
				if now.Sub(lastCheck) >= slowConnectionCheckInterval {
					speed := float64(*offset-lastOffset) / now.Sub(lastCheck).Seconds()
					avg, peers := d.updateRangeSpeed(speedID, speed)
					remaining := active.end.Load() - *offset + 1
					if shouldCloseSlowConnection(speed, avg, peers, now.Sub(started), *offset-requestStart, remaining) {
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
						return state.finish(errSlowConnection)
					}
				}
			}
			if writeSize < int64(n) {
				return state.flush()
			}
		}

		if readErr == io.EOF {
			if *offset > active.end.Load() {
				return state.flush()
			}
			return state.finish(io.ErrUnexpectedEOF)
		}
		if readErr != nil {
			if *offset > end {
				return state.flush()
			}
			return state.finish(readErr)
		}
	}
}

type rangeWriteState struct {
	d           *downloader
	writer      io.WriterAt
	active      *activePart
	offset      *int64
	buffered    bool
	bufferStart int64
	buffer      []byte
}

func (s *rangeWriteState) write(data []byte) (int64, error) {
	s.active.mu.Lock()
	defer s.active.mu.Unlock()

	end := s.active.end.Load()
	if *s.offset > end {
		return 0, nil
	}
	writeSize := min(int64(len(data)), end-*s.offset+1)
	if writeSize <= 0 {
		return 0, nil
	}
	if s.buffered {
		if len(s.buffer) == 0 {
			s.bufferStart = *s.offset
		}
		s.buffer = append(s.buffer, data[:int(writeSize)]...)
		*s.offset += writeSize
		s.active.offset.Store(*s.offset)
		return writeSize, nil
	}

	written, err := s.writer.WriteAt(data[:int(writeSize)], *s.offset)
	if err != nil {
		return 0, err
	}
	if int64(written) != writeSize {
		return 0, io.ErrShortWrite
	}
	*s.offset += writeSize
	s.active.offset.Store(*s.offset)
	s.d.addProgress(writeSize, 0)
	return writeSize, nil
}

func (s *rangeWriteState) finish(err error) error {
	if flushErr := s.flush(); flushErr != nil {
		return flushErr
	}
	return err
}

func (s *rangeWriteState) flush() error {
	if len(s.buffer) == 0 {
		return nil
	}
	written, err := s.writer.WriteAt(s.buffer, s.bufferStart)
	if err != nil {
		return err
	}
	if written != len(s.buffer) {
		return io.ErrShortWrite
	}
	s.d.addProgress(int64(len(s.buffer)), 0)
	s.buffer = s.buffer[:0]
	return nil
}

func (d *downloader) recordIPAttempt(key string, bytes int64, elapsed time.Duration, err error) {
	if d.selector != nil {
		d.selector.recordIP(key, bytes, elapsed, err)
	}
}

func shouldCloseSlowConnection(speed float64, avg float64, peers int, age time.Duration, bytes int64, remaining int64) bool {
	if peers >= slowConnectionMinPeers &&
		age >= slowConnectionMinAge &&
		bytes >= slowConnectionMinBytes &&
		avg > 0 &&
		speed > 0 &&
		speed < avg*slowConnectionRatio {
		return true
	}
	return remaining > 0 &&
		remaining <= slowTailWindow &&
		age >= slowTailMinAge &&
		bytes >= slowTailMinBytes &&
		speed > 0 &&
		speed < minLeasedPartSpeed
}

type discardWriterAt struct{}

func (discardWriterAt) WriteAt(p []byte, _ int64) (int, error) {
	return len(p), nil
}
