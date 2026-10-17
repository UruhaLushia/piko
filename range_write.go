package piko

import (
	"context"
	"io"
	"net/http/httptrace"
	"time"
)

const (
	rangeWriteBufferSize = 256 * 1024
	maxBufferedRangeSize = 512 * 1024
)

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

type rangeBuffers struct {
	read, write []byte
}

func (d *downloader) copyRangeBody(ctx context.Context, abort func(), writer io.WriterAt, reader io.Reader, offset *int64, active *activePart, lease time.Duration, connection httptrace.GotConnInfo, ipKey string, buffers *rangeBuffers, buffered bool, probeIdleTimeout time.Duration, probeProgress func(int64)) error {
	state := rangeWriteState{
		d:        d,
		writer:   writer,
		active:   active,
		offset:   offset,
		buffered: buffered,
	}
	if buffered {
		size := int(min(active.part.length(), int64(maxBufferedRangeSize)))
		if cap(buffers.write) < size {
			buffers.write = make([]byte, 0, size)
		}
		state.buffer = buffers.write[:0]
	}
	size := int(min(active.part.length(), int64(rangeWriteBufferSize)))
	if len(buffers.read) < size {
		buffers.read = make([]byte, size)
	}
	buf := buffers.read
	progress := d.startStallMonitor(abort)
	if progress != nil {
		defer close(progress)
	}
	stopLease := startLeaseMonitor(abort, lease)
	defer stopLease()
	probeTimer := newIdleTimer(probeIdleTimeout, abort)
	defer probeTimer.stop()
	stopSpeed, slow := d.startRangeSpeedMonitor(ctx, active, *offset, connection, ipKey, abort)
	defer stopSpeed()
	sampled := false

	for {
		if err := ctx.Err(); err != nil {
			if slowErr := slow.err(); slowErr != nil {
				return state.finish(slowErr)
			}
			if probeTimer.expired() {
				return state.finish(errRateProbeTimeout)
			}
			return state.finish(err)
		}

		end := active.end.Load()
		if *offset > end {
			return state.flush()
		}
		readSize := min(int64(len(buf)), end-*offset+1)
		n, readErr := reader.Read(buf[:int(readSize)])
		if n > 0 {
			probeTimer.reset(probeIdleTimeout)
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
			if probeProgress != nil {
				probeProgress(writeSize)
			}
			if !sampled && active.sample != nil {
				sampled = active.sample()
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
			if slowErr := slow.err(); slowErr != nil {
				return state.finish(slowErr)
			}
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

	end := s.active.end.Load()
	if *s.offset > end {
		s.active.mu.Unlock()
		return 0, nil
	}
	writeSize := min(int64(len(data)), end-*s.offset+1)
	if writeSize <= 0 {
		s.active.mu.Unlock()
		return 0, nil
	}
	if s.buffered {
		if len(s.buffer) == 0 {
			s.bufferStart = *s.offset
		}
		s.buffer = append(s.buffer, data[:int(writeSize)]...)
		*s.offset += writeSize
		s.active.offset.Store(*s.offset)
		s.active.mu.Unlock()
		return writeSize, nil
	}

	// Reserve ownership before disk backpressure can block this worker.
	s.active.offset.Store(*s.offset + writeSize)
	s.active.mu.Unlock()
	written, err := s.writeAt(data[:int(writeSize)], *s.offset)
	if err != nil {
		return 0, rangeWriteError{err}
	}
	if int64(written) != writeSize {
		return 0, rangeWriteError{io.ErrShortWrite}
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
	written, err := s.writeAt(s.buffer, s.bufferStart)
	if err != nil {
		return rangeWriteError{err}
	}
	if written != len(s.buffer) {
		return rangeWriteError{io.ErrShortWrite}
	}
	s.d.addProgress(int64(len(s.buffer)), 0)
	s.buffer = s.buffer[:0]
	return nil
}

func (s *rangeWriteState) writeAt(data []byte, offset int64) (int, error) {
	s.active.writing.Store(true)
	started := time.Now()
	written, err := s.writer.WriteAt(data, offset)
	s.active.writeNanos.Add(time.Since(started).Nanoseconds())
	s.active.writing.Store(false)
	return written, err
}

func shouldCloseSlowConnection(speed, avg, otherAvg float64, peers int, bytes int64) bool {
	if otherAvg > 0 {
		return speed < otherAvg*slowConnectionSourceRatio
	}
	return peers >= slowConnectionMinPeers && bytes >= slowConnectionMinBytes && speed > 0 && speed < avg*slowConnectionRatio
}

type discardWriterAt struct{}

func (discardWriterAt) WriteAt(p []byte, _ int64) (int, error) {
	return len(p), nil
}
