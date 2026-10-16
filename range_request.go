package piko

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"github.com/UruhaLushia/piko/internal/dialer"
)

type rangeConnection struct {
	mu     sync.Mutex
	info   httptrace.GotConnInfo
	key    string
	direct bool
	closed bool
	d      *downloader
	state  *rangeConnectionState
}

type firstByteReader struct {
	io.Reader
	first func()
}

func (r *firstByteReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 && r.first != nil {
		r.first()
		r.first = nil
	}
	return n, err
}

func (c *rangeConnection) set(info httptrace.GotConnInfo) {
	key := ""
	if info.Conn != nil {
		key = dialer.RemoteAddrIPKey(info.Conn.RemoteAddr())
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.d.retireConnection(info.Conn)
		return
	}
	if info.Conn != c.info.Conn {
		c.d.releaseConnection(c.info.Conn, c.state)
		c.state = c.d.trackConnection(info.Conn)
	}
	c.info = info
	c.key = ""
	if c.direct {
		c.key = key
	}
	c.mu.Unlock()
}

func (c *rangeConnection) snapshot() (httptrace.GotConnInfo, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.info, c.key
}

func (c *rangeConnection) close() {
	c.mu.Lock()
	c.closed = true
	conn := c.info.Conn
	c.mu.Unlock()
	if conn != nil {
		c.d.retireConnection(conn)
	}
}

func (c *rangeConnection) replaced() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.info.Conn == nil {
		return false
	}
	return c.state != nil && c.state.retired.Load()
}

func (c *rangeConnection) release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.d.releaseConnection(c.info.Conn, c.state)
	c.info.Conn, c.state = nil, nil
}

func (d *downloader) trackConnection(conn net.Conn) *rangeConnectionState {
	if conn == nil {
		return nil
	}
	d.connectionMu.Lock()
	defer d.connectionMu.Unlock()
	if d.connections == nil {
		d.connections = make(map[net.Conn]*rangeConnectionState)
	}
	state := d.connections[conn]
	if state == nil {
		state = &rangeConnectionState{}
		d.connections[conn] = state
	}
	state.users++
	return state
}

func (d *downloader) releaseConnection(conn net.Conn, state *rangeConnectionState) {
	if state == nil {
		return
	}
	d.connectionMu.Lock()
	defer d.connectionMu.Unlock()
	state.users--
	if state.users == 0 {
		delete(d.connections, conn)
	}
}

func (d *downloader) retireConnection(conn net.Conn) {
	if conn != nil {
		// Closing a shared HTTP/2 connection also interrupts unrelated streams.
		d.connectionMu.Lock()
		if state := d.connections[conn]; state != nil {
			state.retired.Store(true)
		}
		d.connectionMu.Unlock()
		closeConn(conn)
	}
}

func closeConn(conn net.Conn) {
	if conn != nil {
		_ = conn.Close()
	}
}

func (d *downloader) downloadRange(ctx context.Context, client *http.Client, writer io.WriterAt, active *activePart, source int, buffers *rangeBuffers, probeIdleTimeout time.Duration, probeProgress func(int64)) (int64, error) {
	p := active.part
	p.source = source
	offset := p.start
	if d.hedgeEnabled && !d.hedgeMismatch.Load() && len(d.sources) > 1 {
		probePart := p
		probePart.end = active.end.Load()
		data, err := d.hedgeRange(ctx, client, probePart)
		if err != nil {
			return offset, err
		}
		// The scheduler may have handed the tail to another worker while the race ran.
		count := min(int64(len(data)), max(active.end.Load()-offset+1, 0))
		if count > 0 {
			n, writeErr := writer.WriteAt(data[:count], offset)
			offset += int64(n)
			active.offset.Store(offset)
			d.addProgress(int64(n), d.total)
			if probeProgress != nil {
				probeProgress(int64(n))
			}
			if writeErr != nil {
				return offset, rangeWriteError{writeErr}
			}
			if int64(n) != count {
				return offset, rangeWriteError{io.ErrShortWrite}
			}
		}
	}
	if d.hedgeMismatch.Load() {
		return offset, ErrSourceMismatch
	}
	var lastErr error
	for attempt := 0; attempt <= d.retries; attempt++ {
		end := active.end.Load()
		if offset > end {
			return offset, nil
		}
		if err := ctx.Err(); err != nil {
			return offset, err
		}
		attemptStart := offset
		var attemptCanceled bool
		offset, attemptCanceled, lastErr = d.downloadRangeAttempt(ctx, client, writer, active, p, offset, end, buffers, probeIdleTimeout, probeProgress)
		if lastErr == nil || offset > active.end.Load() {
			return offset, nil
		}
		if active.connectionCloseRequested() {
			return offset, errConcurrencyProbeClosed
		}
		if len(d.sources) > 1 || !isRetryableDownloadError(lastErr) || isRateLimitedDownloadError(lastErr) {
			return offset, lastErr
		}
		// The scheduler redistributes partial progress and internally canceled attempts.
		if ctx.Err() == nil && (offset > attemptStart || attemptCanceled) {
			return offset, lastErr
		}

		if attempt < d.retries {
			if err := sleepWithContext(ctx, retryDelay(attempt)); err != nil {
				return offset, err
			}
		}
	}
	return offset, fmt.Errorf("part %d failed at byte %d: %w", p.index, offset, lastErr)
}

func (d *downloader) downloadRangeAttempt(ctx context.Context, client *http.Client, writer io.WriterAt, active *activePart, p part, start, end int64, buffers *rangeBuffers, probeIdleTimeout time.Duration, probeProgress func(int64)) (offset int64, canceled bool, resultErr error) {
	offset = start
	active.offset.Store(offset)
	attemptCtx, cancel := context.WithCancel(ctx)
	connInfo := &rangeConnection{d: d}
	abort := func() { cancel(); connInfo.close() }
	probeTimer := newIdleTimer(probeIdleTimeout, abort)
	defer func() {
		probeTimer.stop()
		active.clearConnection()
		cancel()
		connInfo.release()
	}()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, d.urlOf(p.source), nil)
	if err != nil {
		return offset, false, err
	}
	d.setCommonHeaders(req)
	d.setSourceHeaders(req, p.source)
	remoteStart, remoteEnd := d.rangeOffset+start, d.rangeOffset+end
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", remoteStart, remoteEnd))
	if validator := d.resumeValidator(); validator != "" && p.source == d.primarySource {
		req.Header.Set("If-Range", validator)
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GetConn: func(address string) {
			// The first hop may be a proxy shared by several origins.
			port := req.URL.Port()
			if port == "" {
				port = "80"
				if req.URL.Scheme == "https" {
					port = "443"
				}
			}
			connInfo.mu.Lock()
			connInfo.direct = strings.EqualFold(address, net.JoinHostPort(req.URL.Hostname(), port))
			connInfo.mu.Unlock()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			connInfo.set(info)
			active.setConnection(info.Conn)
		},
	}))
	started := time.Now()
	bodyStarted := started
	bodyWriteNanos := active.writeNanos.Load()
	bodyElapsed := func() time.Duration {
		return max(time.Since(bodyStarted)-time.Duration(active.writeNanos.Load()-bodyWriteNanos), time.Nanosecond)
	}
	headerBudget := d.responseHeaderBudget(p.source, end-start+1)
	headerTimer := newIdleTimer(headerBudget, abort)
	defer headerTimer.stop()
	d.recordSourceAttempt(p.source)
	defer func() {
		_, writeFailed := errors.AsType[rangeWriteError](resultErr)
		if resultErr != nil && !writeFailed && active.connectionCloseRequested() {
			resultErr = errConcurrencyProbeClosed
		}
		_, key := connInfo.snapshot()
		if !writeFailed && !errors.Is(resultErr, errConcurrencyProbeClosed) && !errors.Is(resultErr, errConnectionReplaced) {
			d.selector.RecordIP(key, offset-start, bodyElapsed(), bodyStarted.Sub(started), resultErr)
		}
		d.finishSource(p.source, offset-start, bodyElapsed(), resultErr)
	}()
	resp, err := client.Do(req)
	headerTimer.stop()
	probeTimer.stop()
	if headerTimer.expired() && ctx.Err() == nil {
		d.extendResponseHeaderBudget(p.source, headerBudget)
		err = errResponseHeaderTimeout
	}
	if err != nil {
		if probeTimer.expired() && ctx.Err() == nil {
			err = errRateProbeTimeout
		}
		if !headerTimer.expired() && !probeTimer.expired() && connInfo.replaced() {
			err = errConnectionReplaced
		}
		connInfo.close()
		if resp != nil {
			resp.Body.Close()
		}
		return offset, attemptCtx.Err() != nil, err
	}
	bodyStarted = time.Now()
	reader := &firstByteReader{Reader: resp.Body, first: func() {
		d.recordSourceTTFB(p.source, time.Since(started))
		bodyStarted = time.Now()
		bodyWriteNanos = active.writeNanos.Load()
		active.mu.Lock()
		active.bodyStarted = bodyStarted
		active.bodyOffset = offset
		active.bodyWriteNanos = bodyWriteNanos
		active.mu.Unlock()
	}}
	connection, key := connInfo.snapshot()
	if err = validateRangeResponse(resp, p.index, remoteStart, remoteEnd, d.remoteSize); err == nil {
		buffered := shouldBufferRangeWrite(writer, end-start+1)
		err = d.copyRangeBody(attemptCtx, abort, writer, reader, &offset, active, partLease(p), connection, key, buffers, buffered, probeIdleTimeout, probeProgress)
		if _, writeFailed := errors.AsType[rangeWriteError](err); err != nil && !writeFailed && connInfo.replaced() {
			err = errConnectionReplaced
		}
	}
	canceled = attemptCtx.Err() != nil
	if shouldCloseRangeConnection(err, offset, end, active.end.Load()) {
		abort()
	}
	resp.Body.Close()
	if err == nil && d.completedRangeSlowed(p.source, offset-start, bodyElapsed()) {
		client.CloseIdleConnections()
	}
	return offset, canceled, err
}

func shouldCloseRangeConnection(err error, offset int64, requestEnd int64, activeEnd int64) bool {
	if err != nil {
		return true
	}
	return activeEnd < requestEnd || offset <= requestEnd
}
