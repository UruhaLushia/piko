package piko

import (
	"fmt"
	"net"
	"strings"
	"sync/atomic"
)

type ConnectionStrategy int

const (
	ConnectionStrategySequential ConnectionStrategy = iota
	ConnectionStrategyRoundRobin
	ConnectionStrategyFastest
)

func (s ConnectionStrategy) String() string {
	switch s {
	case ConnectionStrategySequential:
		return "sequential"
	case ConnectionStrategyRoundRobin:
		return "round-robin"
	case ConnectionStrategyFastest:
		return "fastest"
	default:
		return "unknown"
	}
}

func ParseConnectionStrategy(value string) (ConnectionStrategy, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "_", "-")

	switch normalized {
	case "", "default", "sequential", "ordered", "first":
		return ConnectionStrategySequential, nil
	case "rr", "roundrobin", "round-robin":
		return ConnectionStrategyRoundRobin, nil
	case "fast", "fastest", "race", "racing":
		return ConnectionStrategyFastest, nil
	default:
		return ConnectionStrategySequential, fmt.Errorf("unknown connection strategy %q (use sequential, round-robin, or fastest)", value)
	}
}

type dialIPSelector struct {
	strategy ConnectionStrategy
	next     atomic.Uint64
}

func newDialIPSelector(strategy ConnectionStrategy) *dialIPSelector {
	return &dialIPSelector{strategy: strategy}
}

func (s *dialIPSelector) order(ips []net.IPAddr) []net.IPAddr {
	if s == nil || s.strategy == ConnectionStrategySequential || len(ips) < 2 {
		return ips
	}

	start := int(s.next.Add(1)-1) % len(ips)
	if start == 0 {
		return ips
	}

	ordered := make([]net.IPAddr, 0, len(ips))
	ordered = append(ordered, ips[start:]...)
	ordered = append(ordered, ips[:start]...)
	return ordered
}
