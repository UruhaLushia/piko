package piko

import (
	"context"
	"io"
	"os"
	"sync"
)

const asyncWriteQueueSize = 128

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

type asyncFileWriterAt struct {
	file *os.File
	ch   chan asyncFileWrite
	done chan struct{}

	mu     sync.Mutex
	err    error
	closed bool
}

type asyncFileWrite struct {
	offset int64
	data   []byte
}

func newAsyncFileWriterAt(file *os.File) *asyncFileWriterAt {
	writer := &asyncFileWriterAt{
		file: file,
		ch:   make(chan asyncFileWrite, asyncWriteQueueSize),
		done: make(chan struct{}),
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

	data := append([]byte(nil), p...)
	w.ch <- asyncFileWrite{offset: offset, data: data}
	return len(p), nil
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
		if w.error() != nil {
			continue
		}
		written, err := w.file.WriteAt(write.data, write.offset)
		if err != nil {
			w.setError(err)
			continue
		}
		if written != len(write.data) {
			w.setError(io.ErrShortWrite)
		}
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
