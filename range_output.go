package piko

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
)

const asyncWriteQueueSize = 8

func (d *downloader) downloadParts(ctx context.Context, output string, size int64, partSize int64, concurrency int, force bool) error {
	discard := IsNullOutput(output)
	resume := d.resume
	partPath := ""
	var writer io.WriterAt = discardWriterAt{}
	var file *os.File
	var asyncWriter *asyncFileWriterAt
	var tracker *resumeTracker
	var pending []downloadSpan
	var completed int64
	if !discard {
		partPath = output + ".part"
		var err error
		if resume {
			var completedSpans []downloadSpan
			file, completedSpans, tracker, err = openResumeFile(partPath, d.resumeIdentity(size))
			if err != nil {
				return err
			}
			pending = missingDownloadSpans(size, completedSpans)
			completed = downloadSpanBytes(completedSpans)
		} else {
			if err := prepareTemp(partPath); err != nil {
				return err
			}
			_ = os.Remove(resumeStatePath(partPath))
			file, err = os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				return err
			}
			if err := file.Truncate(size); err != nil {
				file.Close()
				_ = os.Remove(partPath)
				return err
			}
		}
		var committed func(int64, int) error
		if tracker != nil {
			committed = tracker.Record
		}
		asyncWriter = newAsyncFileWriterAt(file, committed)
		writer = asyncWriter
	}

	var err error
	if resume && !discard {
		err = d.downloadPartsToWriterSpans(ctx, writer, size, partSize, concurrency, pending, completed)
	} else {
		err = d.downloadPartsToWriter(ctx, writer, size, partSize, concurrency)
	}
	if asyncWriter != nil {
		err = preferOutputError(err, asyncWriter.Close())
	}
	if tracker != nil {
		err = preferOutputError(err, tracker.Checkpoint())
	}
	err = preferOutputError(err, closeFile(file))
	if err != nil {
		if !discard && !resume {
			_ = os.Remove(partPath)
		}
		return err
	}

	if discard {
		return nil
	}
	if err := finishOutput(partPath, output, force); err != nil {
		return err
	}
	_ = os.Remove(resumeStatePath(partPath))
	return nil
}

func preferOutputError(current, cleanup error) error {
	if cleanup != nil && (current == nil || errors.Is(current, context.Canceled) || errors.Is(current, context.DeadlineExceeded)) {
		return cleanup
	}
	return current
}

type asyncFileWriterAt struct {
	file      *os.File
	committed func(int64, int) error
	ch        chan asyncFileWrite
	buffers   chan []byte
	done      chan struct{}

	mu     sync.Mutex
	err    error
	closed bool
}

type asyncFileWrite struct {
	offset int64
	data   []byte
}

func newAsyncFileWriterAt(file *os.File, committed func(int64, int) error) *asyncFileWriterAt {
	writer := &asyncFileWriterAt{
		file:      file,
		committed: committed,
		ch:        make(chan asyncFileWrite, asyncWriteQueueSize),
		buffers:   make(chan []byte, asyncWriteQueueSize),
		done:      make(chan struct{}),
	}
	for range asyncWriteQueueSize {
		writer.buffers <- nil
	}
	go writer.run()
	return writer
}

func (w *asyncFileWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := w.error(); err != nil {
		return 0, err
	}

	written := 0
	for written < len(p) {
		data := <-w.buffers
		if err := w.error(); err != nil {
			w.buffers <- data
			return written, err
		}
		size := min(len(p)-written, rangeWriteBufferSize)
		if cap(data) < size {
			data = make([]byte, size)
		}
		data = data[:size]
		copy(data, p[written:written+size])
		w.ch <- asyncFileWrite{offset: offset + int64(written), data: data}
		written += size
	}
	return written, nil
}

func (w *asyncFileWriterAt) Close() error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.ch)
	}
	w.mu.Unlock()
	<-w.done
	return w.error()
}

func (w *asyncFileWriterAt) run() {
	defer close(w.done)
	for write := range w.ch {
		if w.error() == nil {
			written, err := w.file.WriteAt(write.data, write.offset)
			if err == nil && written != len(write.data) {
				err = io.ErrShortWrite
			}
			if err == nil && w.committed != nil {
				err = w.committed(write.offset, written)
			}
			w.setError(err)
		}
		w.buffers <- write.data[:0]
	}
}

func (w *asyncFileWriterAt) error() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *asyncFileWriterAt) setError(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		w.err = err
	}
}
