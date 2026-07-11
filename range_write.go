package piko

import (
	"context"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/UruhaLushia/piko/internal/dialer"
)

const (
	rangeWriteBufferSize = 256 * 1024
	maxBufferedRangeSize = 512 * 1024
)

func (d *downloader) copyRange(ctx context.Context, cancel context.CancelFunc, writer io.WriterAt, resp *http.Response, partIndex int, requestStart int64, requestEnd int64, remoteStart int64, remoteEnd int64, offset *int64, active *activePart, lease time.Duration, conn net.Conn, probeIdleTimeout time.Duration, confirmProbe func()) error {
	if resp.StatusCode != http.StatusPartialContent {
		return httpStatusError{partIndex: partIndex, code: resp.StatusCode, status: resp.Status}
	}
	if err := validateContentRange(partIndex, resp.Header.Get("Content-Range"), remoteStart, remoteEnd); err != nil {
		return err
	}

	buffered := shouldBufferRangeWrite(writer, requestEnd-requestStart+1)
	return d.copyRangeBody(ctx, cancel, writer, resp.Body, requestStart, offset, active, lease, conn, buffered, probeIdleTimeout, confirmProbe)
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

func (d *downloader) copyRangeBody(ctx context.Context, cancel context.CancelFunc, writer io.WriterAt, reader io.Reader, requestStart int64, offset *int64, active *activePart, lease time.Duration, conn net.Conn, buffered bool, probeIdleTimeout time.Duration, confirmProbe func()) error {
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
	abort := func() {
		cancel()
		closeConn(conn)
	}
	progress := d.startStallMonitor(abort)
	if progress != nil {
		defer close(progress)
	}
	stopLease := startLeaseMonitor(abort, lease)
	defer stopLease()
	probeTimer := newIdleTimer(probeIdleTimeout, abort)
	defer probeTimer.stop()
	var tailTimer *idleTimer
	defer func() { tailTimer.stop() }()

	speedID := d.registerRangeSpeed()
	defer d.unregisterRangeSpeed(speedID)
	started := time.Now()
	lastCheck := started
	lastOffset := *offset
	slowStrikes := 0
	probeConfirmed := false

	for {
		if err := ctx.Err(); err != nil {
			if probeTimer.expired() {
				return state.finish(errRateProbeTimeout)
			}
			return state.finish(err)
		}

		end := active.end.Load()
		if *offset > end {
			return state.flush()
		}
		if probeIdleTimeout <= 0 && tailTimer == nil && end-*offset+1 <= slowTailWindow {
			tailTimer = newIdleTimer(slowTailIdleTimeout, abort)
		}
		readSize := min(int64(len(buf)), end-*offset+1)
		n, readErr := reader.Read(buf[:int(readSize)])
		if n > 0 {
			probeTimer.reset(probeIdleTimeout)
			tailTimer.reset(slowTailIdleTimeout)
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
			if *offset-requestStart >= concurrencyProbeConfirm && !probeConfirmed && confirmProbe != nil {
				confirmProbe()
				probeConfirmed = true
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
						abort()
						return state.finish(dialer.ErrSlowConnection)
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
			if probeTimer.expired() {
				return state.finish(errRateProbeTimeout)
			}
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
