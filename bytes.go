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
		return nil, Result{SourceStats: d.sourceStats()}, err
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
	result.SourceStats = d.sourceStats()
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

	if len(d.sources) > 1 && opts.Length > 0 {
		info, err := d.inspectSources(ctx)
		if err != nil {
			return nil, Result{SourceStats: d.sourceStats()}, err
		}
		if !info.hasSizedRange() || opts.Offset+opts.Length > info.size {
			return nil, Result{SourceStats: d.sourceStats()}, fmt.Errorf("range outside resource or source is not rangeable")
		}
		d.remoteSize = info.size
		d.url = d.urlOf(d.primarySource)
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
		TotalSize:   d.remoteSize,
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
	result.SourceStats = d.sourceStats()
	return data, result, err
}

func (d *downloader) downloadSingleToBytes(ctx context.Context, total int64) ([]byte, error) {
	var buf bytes.Buffer
	if total > 0 && total <= int64(maxInt()) {
		buf.Grow(int(total))
	}
	err := d.retrySingle(ctx, total, func() error {
		buf.Reset()
		_, err := d.downloadSingleToWriter(ctx, &buf, total)
		return err
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
