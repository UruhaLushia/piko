package piko

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
)

const copyBufferSize = 1024 * 1024

func (d *downloader) downloadSingle(ctx context.Context, output string, total int64, force bool) error {
	discard := IsNullOutput(output)
	partPath := ""
	if !discard {
		partPath = output + ".part"
		if err := prepareTemp(partPath); err != nil {
			return err
		}
	}

	var lastErr error
	for attempt := 0; attempt <= d.retries; attempt++ {
		d.done.Store(0)
		if attempt > 0 && !discard {
			if err := prepareTemp(partPath); err != nil {
				return err
			}
		}

		err := d.downloadSingleAttempt(ctx, partPath, total, discard)
		if err == nil {
			d.emitProgress(total, true)
			if discard {
				return nil
			}
			return finishOutput(partPath, output, force)
		}
		lastErr = err
		if !isRetryableDownloadError(err) || attempt == d.retries {
			break
		}
		if err := sleepWithContext(ctx, retryDelay(attempt)); err != nil {
			return err
		}
	}
	if !discard {
		_ = os.Remove(partPath)
	}
	return lastErr
}

func (d *downloader) downloadSingleAttempt(ctx context.Context, partPath string, total int64, discard bool) error {
	var writer io.Writer = io.Discard
	var file *os.File
	var err error
	if !discard {
		file, err = os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		writer = file
	}

	copied, err := d.downloadSingleToWriter(ctx, writer, total)
	if closeErr := closeFile(file); err == nil {
		err = closeErr
	}
	if err == nil && total > 0 && copied != total {
		err = io.ErrUnexpectedEOF
	}
	if err != nil && !discard {
		_ = os.Remove(partPath)
	}
	return err
}

func (d *downloader) downloadSingleToWriter(ctx context.Context, writer io.Writer, total int64) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.url, nil)
	if err != nil {
		return 0, err
	}
	d.setCommonHeaders(req)

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

	copied, err := d.copyToWriter(attemptCtx, cancel, writer, resp.Body, total)
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
