package piko

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/UruhaLushia/piko/internal/dialer"
)

const statusBandwidthLimitExceeded = 509

var errConnectionDegraded = dialer.ErrConnectionDegraded

type rangeWriteError struct{ err error }

func (e rangeWriteError) Error() string { return e.err.Error() }
func (e rangeWriteError) Unwrap() error { return e.err }

type httpStatusError struct {
	partIndex int
	code      int
	status    string
}

func (e httpStatusError) Error() string {
	return fmt.Sprintf("part %d expected 206, got %s", e.partIndex, e.status)
}

type contentRangeError struct {
	partIndex int
	value     string
	expected  string
}

var (
	errRateProbeTimeout       = errors.New("rate probe timeout")
	errResponseHeaderTimeout  = errors.New("download response headers stalled")
	errSourceExploration      = errors.New("trying an unmeasured download source")
	errConnectionReplaced     = errors.New("shared download connection replaced")
	errConcurrencyProbeClosed = errors.New("concurrency probe connection closed")
)

func (e contentRangeError) Error() string {
	return fmt.Sprintf("part %d invalid Content-Range %q, expected %s", e.partIndex, e.value, e.expected)
}

func validateRangeResponse(resp *http.Response, partIndex int, start, end, total int64) error {
	if resp.StatusCode != http.StatusPartialContent {
		return httpStatusError{partIndex: partIndex, code: resp.StatusCode, status: resp.Status}
	}
	return validateContentRange(partIndex, resp.Header.Get("Content-Range"), start, end, total)
}

func validateContentRange(partIndex int, value string, expectedStart int64, expectedEnd int64, total int64) error {
	expected := formatByteRange(expectedStart, expectedEnd)
	if total > 0 {
		expected = fmt.Sprintf("bytes %d-%d/%d", expectedStart, expectedEnd, total)
	}
	invalid := contentRangeError{partIndex: partIndex, value: value, expected: expected}
	if !strings.HasPrefix(value, "bytes ") {
		return invalid
	}
	span, size, ok := strings.Cut(strings.TrimPrefix(value, "bytes "), "/")
	if !ok {
		return invalid
	}
	a, b, ok := strings.Cut(span, "-")
	if !ok {
		return invalid
	}
	start, e1 := strconv.ParseInt(a, 10, 64)
	end, e2 := strconv.ParseInt(b, 10, 64)
	if e1 != nil || e2 != nil || start < 0 || end < start || start != expectedStart || end != expectedEnd {
		return invalid
	}
	if size == "*" && total <= 0 {
		return nil
	}
	actual, err := strconv.ParseInt(size, 10, 64)
	if err != nil || actual <= end || (total > 0 && actual != total) {
		return invalid
	}
	return nil
}

func formatByteRange(start int64, end int64) string {
	return fmt.Sprintf("bytes %d-%d/*", start, end)
}

func isRetryableDownloadError(err error) bool {
	if err == nil {
		return false
	}
	if _, writeFailed := errors.AsType[rangeWriteError](err); writeFailed {
		return false
	}
	if statusErr, ok := errors.AsType[httpStatusError](err); ok {
		return statusErr.code == http.StatusTooManyRequests || statusErr.code >= 500
	}
	return true
}

func isRateLimitedDownloadError(err error) bool {
	var statusErr httpStatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	return statusErr.code == http.StatusTooManyRequests ||
		statusErr.code == http.StatusServiceUnavailable ||
		statusErr.code == statusBandwidthLimitExceeded
}

func isRateProbeTimeout(err error) bool {
	return errors.Is(err, errRateProbeTimeout)
}

func isSlowConnectionError(err error) bool {
	return errors.Is(err, dialer.ErrSlowConnection)
}

func isTransientNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if _, ok := errors.AsType[httpStatusError](err); ok {
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

func isTransientRangeError(err error) bool {
	return isTransientNetworkError(err) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}
