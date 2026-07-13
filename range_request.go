package piko

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"

	"github.com/UruhaLushia/piko/internal/dialer"
)

type rangeConnection struct {
	mu     sync.Mutex
	conn   net.Conn
	key    string
	closed bool
}

func (c *rangeConnection) set(conn net.Conn) {
	key := ""
	if conn != nil {
		key = dialer.RemoteAddrIPKey(conn.RemoteAddr())
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		closeConn(conn)
		return
	}
	c.conn = conn
	c.key = key
	c.mu.Unlock()
}

func (c *rangeConnection) snapshot() (net.Conn, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn, c.key
}

func (c *rangeConnection) close() {
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	closeConn(conn)
}

func closeConn(conn net.Conn) {
	if conn != nil {
		_ = conn.Close()
	}
}

func (d *downloader) downloadRange(ctx context.Context, client *http.Client, writer io.WriterAt, active *activePart, probeIdleTimeout time.Duration, confirmProbe func()) (int64, error) {
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
		abortAttempt := func() {
			attemptCancel()
			connInfo.close()
		}
		probeTimer := newIdleTimer(probeIdleTimeout, abortAttempt)
		attemptCtx = httptrace.WithClientTrace(attemptCtx, &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				connInfo.set(info.Conn)
				active.setConnection(info.Conn)
			},
		})
		finishAttempt := func() {
			active.clearConnection()
			attemptCancel()
		}
		req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, d.url, nil)
		if err != nil {
			probeTimer.stop()
			finishAttempt()
			return offset, err
		}
		d.setCommonHeaders(req)

		attemptStart := offset
		attemptStarted := time.Now()
		remoteStart := d.rangeOffset + offset
		remoteEnd := d.rangeOffset + end
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", remoteStart, remoteEnd))
		if validator := d.resumeValidator(); validator != "" {
			req.Header.Set("If-Range", validator)
		}
		resp, err := client.Do(req)
		probeTimer.stop()
		conn, connKey := connInfo.snapshot()
		if err != nil {
			if probeTimer.expired() && ctx.Err() == nil {
				err = errRateProbeTimeout
			}
			connInfo.close()
			if resp != nil {
				resp.Body.Close()
			}
			d.recordIPAttempt(connKey, offset-attemptStart, time.Since(attemptStarted), err)
			attemptCanceled := attemptCtx.Err() != nil
			finishAttempt()
			if offset > active.end.Load() {
				return offset, nil
			}
			lastErr = err
			if active.connectionCloseRequested() {
				return offset, errConcurrencyProbeClosed
			}
			if !isRetryableDownloadError(err) {
				return offset, err
			}
			if ctx.Err() == nil && attemptCanceled {
				return offset, err
			}
		} else {
			err = d.copyRange(attemptCtx, attemptCancel, writer, resp, p.index, attemptStart, end, remoteStart, remoteEnd, &offset, active, partLease(p), conn, probeIdleTimeout, confirmProbe)
			attemptCanceled := attemptCtx.Err() != nil
			if shouldCloseRangeConnection(err, offset, end, active.end.Load()) {
				abortAttempt()
			}
			resp.Body.Close()
			d.recordIPAttempt(connKey, offset-attemptStart, time.Since(attemptStarted), err)
			finishAttempt()
			if err == nil {
				return offset, nil
			}
			if offset > active.end.Load() {
				return offset, nil
			}
			lastErr = err
			if active.connectionCloseRequested() {
				return offset, errConcurrencyProbeClosed
			}
			if !isRetryableDownloadError(err) {
				return offset, err
			}
			if isRateLimitedDownloadError(err) {
				return offset, err
			}
			if ctx.Err() == nil && (offset > attemptStart || attemptCanceled) {
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

func (d *downloader) recordIPAttempt(key string, bytes int64, elapsed time.Duration, err error) {
	if d.selector != nil {
		d.selector.RecordIP(key, bytes, elapsed, err)
	}
}
