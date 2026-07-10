package piko

import (
	"context"
	"strconv"
	"strings"
	"time"
)

func parseContentRangeSize(value string) int64 {
	slash := strings.LastIndexByte(value, '/')
	if slash < 0 || slash == len(value)-1 {
		return -1
	}
	sizeText := strings.TrimSpace(value[slash+1:])
	if sizeText == "*" {
		return -1
	}
	size, err := strconv.ParseInt(sizeText, 10, 64)
	if err != nil {
		return -1
	}
	return size
}

func retryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return 250 * time.Millisecond
	}
	if attempt >= 4 {
		return 3 * time.Second
	}
	delay := time.Duration(250*(1<<attempt)) * time.Millisecond
	return delay
}

func rateLimitDelay(requeues int) time.Duration {
	if requeues < 0 {
		requeues = 0
	}
	delay := time.Duration(2*(1<<min(requeues, 4))) * time.Second
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
