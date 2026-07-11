package piko

import (
	"bytes"
	"context"
	"fmt"
	"io"
)

func (d *downloader) runBytes(ctx context.Context, opts Options) ([]byte, Result, error) {
	result, err := d.plan(ctx, opts, false)
	if err != nil {
		return nil, Result{}, err
	}

	if opts.Started != nil {
		opts.Started(result)
	}

	d.total = result.Size
	var data []byte
	if result.Segmented {
		data, err = d.downloadPartsToBytes(ctx, result.Size, result.PartSize, result.Connections)
	} else {
		data, err = d.downloadSingleToBytes(ctx, result.Size)
	}
	if err != nil {
		return nil, result, err
	}
	if result.Size <= 0 {
		result.Size = int64(len(data))
	}
	return data, result, nil
}

func (d *downloader) runBytesRange(ctx context.Context, opts Options) ([]byte, Result, error) {
	if opts.Offset < 0 {
		return nil, Result{}, fmt.Errorf("negative offset %d", opts.Offset)
	}
	if opts.Length < 0 {
		return nil, Result{}, fmt.Errorf("negative length %d", opts.Length)
	}
	if opts.Length > (1<<63-1)-opts.Offset {
		return nil, Result{}, fmt.Errorf("byte range overflows: offset %d length %d", opts.Offset, opts.Length)
	}

	connections := opts.Connections
	if opts.Length == 0 {
		connections = 1
	} else {
		bytesPerConnection := max(opts.PartSize, int64(minBytesPerConnection))
		maxUseful := max(int((opts.Length+bytesPerConnection-1)/bytesPerConnection), 1)
		connections = min(connections, maxUseful)
	}
	result := Result{
		Offset:      opts.Offset,
		Size:        opts.Length,
		Rangeable:   true,
		FinalURL:    d.url,
		Connections: connections,
		Parallel:    connections > 1,
		PartSize:    opts.PartSize,
		Segmented:   true,
	}
	if opts.Started != nil {
		opts.Started(result)
	}
	if opts.Length == 0 {
		return []byte{}, result, nil
	}

	d.total = opts.Length
	data, err := d.downloadPartsToBytes(ctx, opts.Length, opts.PartSize, connections)
	if err != nil {
		return nil, result, err
	}
	return data, result, nil
}

func (d *downloader) downloadSingleToBytes(ctx context.Context, total int64) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= d.retries; attempt++ {
		d.done.Store(0)

		var buf bytes.Buffer
		if total > 0 && total <= int64(maxInt()) {
			buf.Grow(int(total))
		}

		copied, err := d.downloadSingleToWriter(ctx, &buf, total)
		if err == nil && total > 0 && copied != total {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			d.emitProgress(total, true)
			return buf.Bytes(), nil
		}
		lastErr = err
		if !isRetryableDownloadError(lastErr) || attempt == d.retries {
			break
		}
		if err := sleepWithContext(ctx, retryDelay(attempt)); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func (d *downloader) downloadPartsToBytes(ctx context.Context, size int64, partSize int64, concurrency int) ([]byte, error) {
	if size < 0 {
		return nil, fmt.Errorf("negative size %d", size)
	}
	if size > int64(maxInt()) {
		return nil, fmt.Errorf("download too large for memory: %d bytes", size)
	}

	data := make([]byte, int(size))
	if err := d.downloadPartsToWriter(ctx, byteSliceWriterAt(data), size, partSize, concurrency); err != nil {
		return nil, err
	}
	return data, nil
}

type byteSliceWriterAt []byte

func (w byteSliceWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 || off > int64(len(w)) {
		return 0, io.ErrShortWrite
	}
	end := off + int64(len(p))
	if end > int64(len(w)) {
		return 0, io.ErrShortWrite
	}
	copy(w[off:end], p)
	return len(p), nil
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
