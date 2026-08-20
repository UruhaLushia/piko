package piko

import (
	"fmt"
	"strings"
)

// ConcurrencyStrategy controls whether range download concurrency can be
// reduced while a download is running.
type ConcurrencyStrategy int

const (
	ConcurrencyStrategyDefault ConcurrencyStrategy = iota
	ConcurrencyStrategyFixed
	ConcurrencyStrategyAdaptive
)

func (s ConcurrencyStrategy) String() string {
	switch s {
	case ConcurrencyStrategyDefault:
		return "default"
	case ConcurrencyStrategyFixed:
		return "fixed"
	case ConcurrencyStrategyAdaptive:
		return "adaptive"
	default:
		return "unknown"
	}
}

func ParseConcurrencyStrategy(value string) (ConcurrencyStrategy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "default", "fixed":
		return ConcurrencyStrategyFixed, nil
	case "adaptive":
		return ConcurrencyStrategyAdaptive, nil
	default:
		return ConcurrencyStrategyFixed, fmt.Errorf("unknown concurrency strategy %q (use fixed or adaptive)", value)
	}
}
