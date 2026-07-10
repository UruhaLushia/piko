package piko

import (
	"fmt"
	"strings"
)

type ConnectionStrategy int

const (
	ConnectionStrategyDefault ConnectionStrategy = iota
	ConnectionStrategySequential
	ConnectionStrategyRoundRobin
	ConnectionStrategyFastest
)

type AddressFamily int

const (
	AddressFamilyAuto AddressFamily = iota
	AddressFamilyIPv4
	AddressFamilyIPv6
	AddressFamilyPreferIPv4
	AddressFamilyPreferIPv6
)

func (s ConnectionStrategy) String() string {
	switch s {
	case ConnectionStrategyDefault:
		return "default"
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

func (f AddressFamily) String() string {
	switch f {
	case AddressFamilyAuto:
		return "auto"
	case AddressFamilyIPv4:
		return "ipv4"
	case AddressFamilyIPv6:
		return "ipv6"
	case AddressFamilyPreferIPv4:
		return "prefer-ipv4"
	case AddressFamilyPreferIPv6:
		return "prefer-ipv6"
	default:
		return "unknown"
	}
}

func ParseConnectionStrategy(value string) (ConnectionStrategy, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "_", "-")

	switch normalized {
	case "", "default", "balanced", "balance":
		return ConnectionStrategyRoundRobin, nil
	case "sequential", "ordered", "first":
		return ConnectionStrategySequential, nil
	case "rr", "roundrobin", "round-robin":
		return ConnectionStrategyRoundRobin, nil
	case "fast", "fastest", "race", "racing":
		return ConnectionStrategyFastest, nil
	default:
		return ConnectionStrategySequential, fmt.Errorf("unknown connection strategy %q (use sequential, round-robin, or fastest)", value)
	}
}

func ParseAddressFamily(value string) (AddressFamily, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "_", "-")

	switch normalized {
	case "", "auto", "dual", "dual-stack", "all", "any":
		return AddressFamilyAuto, nil
	case "4", "v4", "ip4", "ipv4", "tcp4", "ipv4-only", "v4-only", "ip4-only", "only-ipv4", "only-v4", "only-ip4":
		return AddressFamilyIPv4, nil
	case "6", "v6", "ip6", "ipv6", "tcp6", "ipv6-only", "v6-only", "ip6-only", "only-ipv6", "only-v6", "only-ip6":
		return AddressFamilyIPv6, nil
	case "prefer4", "prefer-v4", "prefer-ip4", "prefer-ipv4", "ipv4-preferred", "v4-first":
		return AddressFamilyPreferIPv4, nil
	case "prefer6", "prefer-v6", "prefer-ip6", "prefer-ipv6", "ipv6-preferred", "v6-first":
		return AddressFamilyPreferIPv6, nil
	default:
		return AddressFamilyAuto, fmt.Errorf("unknown IP family %q (use auto, ipv4, ipv6, prefer-ipv4, or prefer-ipv6)", value)
	}
}
