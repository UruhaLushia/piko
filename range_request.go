package piko

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/UruhaLushia/piko/internal/dialer"
)

type rangeConnection struct {
	conn net.Conn
	key  string
}

func closeConn(conn net.Conn) {
	if conn != nil {
		_ = conn.Close()
	}
}

func (d *downloader) downloadRange(ctx context.Context, client *http.Client, writer io.WriterAt, active *activePart, probeIdleTimeout time.Duration) (int64, error) {
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
		probeTimer := newRateProbeTimer(probeIdleTimeout, attemptCancel)
		connInfo := &rangeConnection{}
		attemptCtx = httptrace.WithClientTrace(attemptCtx, &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				connInfo.conn = info.Conn
				if info.Conn != nil {
					connInfo.key = dialer.RemoteAddrIPKey(info.Conn.RemoteAddr())
				}
			},
		})
		finishAttempt := func() {
			attemptCancel()
		}
		req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, d.url, nil)
		if err != nil {
			probeTimer.stop()
			finishAttempt()
			return offset, err
		}
		d.setCommonHeaders(req)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))

		attemptStart := offset
		attemptStarted := time.Now()
		resp, err := client.Do(req)
		probeTimer.stop()
		if err != nil {
			if probeTimer.expired() && ctx.Err() == nil {
				closeConn(connInfo.conn)
				err = errRateProbeTimeout
			}
			if resp != nil {
				resp.Body.Close()
			}
			d.recordIPAttempt(connInfo.key, offset-attemptStart, time.Since(attemptStarted), err)
			attemptCanceled := attemptCtx.Err() != nil
			finishAttempt()
			if offset > active.end.Load() {
				return offset, nil
			}
			lastErr = err
			if !isRetryableDownloadError(err) {
				return offset, err
			}
			if ctx.Err() == nil && attemptCanceled {
				return offset, err
			}
		} else {
			err = d.copyRange(attemptCtx, attemptCancel, writer, resp, p.index, attemptStart, end, &offset, active, partLease(p), connInfo.conn, probeIdleTimeout)
			attemptCanceled := attemptCtx.Err() != nil
			if shouldCloseRangeConnection(err, offset, end, active.end.Load()) {
				attemptCancel()
				closeConn(connInfo.conn)
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
