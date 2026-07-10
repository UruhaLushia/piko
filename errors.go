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
)

const statusBandwidthLimitExceeded = 509

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

var errRateProbeTimeout = errors.New("rate probe timeout")

func (e contentRangeError) Error() string {
	return fmt.Sprintf("part %d invalid Content-Range %q, expected %s", e.partIndex, e.value, e.expected)
}

func validateContentRange(partIndex int, value string, expectedStart int64, expectedEnd int64) error {
	if value == "" {
		return contentRangeError{partIndex: partIndex, value: value, expected: formatByteRange(expectedStart, expectedEnd)}
	}
	space := strings.IndexByte(value, ' ')
	slash := strings.IndexByte(value, '/')
	dash := strings.IndexByte(value, '-')
	if space < 0 || slash < 0 || dash < 0 || !(space < dash && dash < slash) {
		return contentRangeError{partIndex: partIndex, value: value, expected: formatByteRange(expectedStart, expectedEnd)}
	}
	start, err := strconv.ParseInt(value[space+1:dash], 10, 64)
	if err != nil {
		return contentRangeError{partIndex: partIndex, value: value, expected: formatByteRange(expectedStart, expectedEnd)}
	}
	end, err := strconv.ParseInt(value[dash+1:slash], 10, 64)
	if err != nil {
		return contentRangeError{partIndex: partIndex, value: value, expected: formatByteRange(expectedStart, expectedEnd)}
	}
	if start != expectedStart || end != expectedEnd {
		return contentRangeError{partIndex: partIndex, value: value, expected: formatByteRange(expectedStart, expectedEnd)}
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
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var statusErr httpStatusError
	if errors.As(err, &statusErr) {
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

func isTransientNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var statusErr httpStatusError
	if errors.As(err, &statusErr) {
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
