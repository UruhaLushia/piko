package piko

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const copyBufferSize = 1024 * 1024

func (d *downloader) downloadSingle(ctx context.Context, output string, total int64, force bool) error {
	discard := IsNullOutput(output)
	partPath := ""
	if !discard {
		partPath = output + ".part"
		defer func() { _ = os.Remove(partPath) }()
		if err := prepareTemp(partPath); err != nil {
			return err
		}
	}

	err := d.retrySingle(ctx, total, func() error {
		return d.downloadSingleAttempt(ctx, partPath, total, discard)
	})
	if err != nil || discard {
		return err
	}
	return finishOutput(partPath, output, force)
}

func (d *downloader) retrySingle(ctx context.Context, total int64, download func() error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		d.done.Store(0)
		err := download()
		if err == nil {
			d.emitProgress(total, true)
			return nil
		}
		if !isRetryableDownloadError(err) || attempt >= d.retries {
			return err
		}
		if err := sleepWithContext(ctx, retryDelay(attempt)); err != nil {
			return err
		}
	}
}

func (d *downloader) downloadSingleAttempt(ctx context.Context, partPath string, total int64, discard bool) error {
	writer := io.Discard
	var file *os.File
	var err error
	if !discard {
		file, err = os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		writer = file
	}

	_, err = d.downloadSingleToWriter(ctx, writer, total)
	if closeErr := closeFile(file); err == nil {
		err = closeErr
	}
	if err != nil && !discard {
		_ = os.Remove(partPath)
	}
	return err
}

func (d *downloader) downloadSingleToWriter(ctx context.Context, writer io.Writer, total int64) (copied int64, resultErr error) {
	source, err := d.acquireSource(ctx, total)
	if err != nil {
		return 0, err
	}
	defer d.releaseSource(source)
	d.recordSourceAttempt(source)
	started := time.Now()
	defer func() { d.finishSource(source, copied, time.Since(started), resultErr) }()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.urlOf(source), nil)
	if err != nil {
		return 0, err
	}
	d.setCommonHeaders(req)
	d.setSourceHeaders(req, source)

	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	req = req.WithContext(attemptCtx)

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("download failed: %s", resp.Status)
	}
	if total <= 0 {
		total = resp.ContentLength
	}

	reader := &firstByteReader{Reader: resp.Body, first: func() {
		d.recordSourceTTFB(source, time.Since(started))
		started = time.Now()
	}}
	copied, err = d.copyToWriter(attemptCtx, cancel, writer, reader, total)
	if err == nil && total > 0 && copied != total {
		err = io.ErrUnexpectedEOF
	}
	return copied, err
}

func (d *downloader) copyToWriter(ctx context.Context, cancel context.CancelFunc, writer io.Writer, reader io.Reader, total int64) (int64, error) {
	buf := make([]byte, copyBufferSize)
	progress := d.startStallMonitor(cancel)
	if progress != nil {
		defer close(progress)
	}

	var copied int64
	d.emitProgress(total, false)
	for {
		if err := ctx.Err(); err != nil {
			return copied, err
		}
		n, readErr := reader.Read(buf)
		if n > 0 {
			written, writeErr := writer.Write(buf[:n])
			if written > 0 {
				copied += int64(written)
				d.addProgress(int64(written), total)
				if progress != nil {
					select {
					case progress <- struct{}{}:
					default:
					}
				}
			}
			if writeErr != nil {
				return copied, writeErr
			}
			if written != n {
				return copied, io.ErrShortWrite
			}
		}
		if readErr == io.EOF {
			return copied, nil
		}
		if readErr != nil {
			return copied, readErr
		}
	}
}

func closeFile(file *os.File) error {
	if file == nil {
		return nil
	}
	return file.Close()
}
