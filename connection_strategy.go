package piko

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ipQualitySmoothFactor       = 0.35
	ipQualityUnknownWeight      = 4
	ipQualityMaxWeight          = 12
	ipQualityMinSampleBytes     = 256 * 1024
	ipQualityMinSampleDuration  = 300 * time.Millisecond
	ipQualitySlowRatio          = 0.55
	ipQualitySlowThreshold      = 3
	ipQualityFailureThreshold   = 3
	ipQualityQuarantineDuration = 45 * time.Second
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

type dialIPSelector struct {
	strategy      ConnectionStrategy
	addressFamily AddressFamily
	next          atomic.Uint64
	nextWeighted  atomic.Uint64
	next4         atomic.Uint64
	next6         atomic.Uint64
	nextAny       atomic.Uint64

	mu    sync.Mutex
	stats map[string]*ipQuality
}

type ipQuality struct {
	emaBps           float64
	samples          int
	slowStreak       int
	failureStreak    int
	quarantinedUntil time.Time
}

func newDialIPSelector(strategy ConnectionStrategy, addressFamily AddressFamily) *dialIPSelector {
	return &dialIPSelector{strategy: strategy, addressFamily: addressFamily}
}

func (s *dialIPSelector) order(ips []net.IPAddr) []net.IPAddr {
	if s == nil || len(ips) < 2 {
		return ips
	}

	switch s.strategy {
	case ConnectionStrategyRoundRobin:
		return s.roundRobinOrder(ips)
	case ConnectionStrategySequential:
		return s.sequentialOrder(ips)
	default:
		return s.sequentialOrder(ips)
	}
}

func (s *dialIPSelector) sequentialOrder(ips []net.IPAddr) []net.IPAddr {
	ips = s.availableIPs(ips)
	switch s.addressFamily {
	case AddressFamilyPreferIPv4:
		v4, v6, other := splitIPFamilies(ips)
		return s.weightedOrder(appendFamilies(v4, other, v6))
	case AddressFamilyPreferIPv6:
		v4, v6, other := splitIPFamilies(ips)
		return s.weightedOrder(appendFamilies(v6, other, v4))
	default:
		return s.weightedOrder(ips)
	}
}

func (s *dialIPSelector) roundRobinOrder(ips []net.IPAddr) []net.IPAddr {
	ips = s.availableIPs(ips)
	v4, v6, other := splitIPFamilies(ips)
	v4 = rotateIPs(v4, &s.next4)
	v6 = rotateIPs(v6, &s.next6)
	other = rotateIPs(other, &s.nextAny)

	switch {
	case len(v4) > 0 && len(v6) > 0:
		if s.addressFamily == AddressFamilyPreferIPv4 {
			return s.weightedOrder(appendInterleaved(v4, v6, other))
		}
		if s.addressFamily == AddressFamilyPreferIPv6 {
			return s.weightedOrder(appendInterleaved(v6, v4, other))
		}
		if s.next.Add(1)%2 == 1 {
			return s.weightedOrder(appendInterleaved(v4, v6, other))
		}
		return s.weightedOrder(appendInterleaved(v6, v4, other))
	case len(v4) > 0:
		return s.weightedOrder(append(append([]net.IPAddr{}, v4...), other...))
	case len(v6) > 0:
		return s.weightedOrder(append(append([]net.IPAddr{}, v6...), other...))
	default:
		return s.weightedOrder(other)
	}
}

func (s *dialIPSelector) availableIPs(ips []net.IPAddr) []net.IPAddr {
	if s == nil || len(ips) < 2 {
		return ips
	}

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.stats) == 0 {
		return ips
	}

	available := make([]net.IPAddr, 0, len(ips))
	for _, ip := range ips {
		stat := s.stats[ipAddrKey(ip)]
		if stat == nil || stat.quarantinedUntil.Before(now) {
			available = append(available, ip)
		}
	}
	if len(available) == 0 {
		return ips
	}
	return available
}

func (s *dialIPSelector) weightedOrder(ips []net.IPAddr) []net.IPAddr {
	if s == nil || len(ips) < 2 {
		return ips
	}

	candidates, known := s.ipCandidates(ips)
	if !known {
		return ips
	}

	totalWeight := 0
	for _, candidate := range candidates {
		totalWeight += candidate.weight
	}
	pick := int(s.nextWeighted.Add(1)-1) % totalWeight

	selected := 0
	for i, candidate := range candidates {
		if pick < candidate.weight {
			selected = i
			break
		}
		pick -= candidate.weight
	}

	chosen := candidates[selected]
	rest := append(candidates[:selected:selected], candidates[selected+1:]...)
	sort.SliceStable(rest, func(i, j int) bool {
		return rest[i].score > rest[j].score
	})

	ordered := make([]net.IPAddr, 0, len(candidates))
	ordered = append(ordered, chosen.ip)
	for _, candidate := range rest {
		ordered = append(ordered, candidate.ip)
	}
	return ordered
}

type ipCandidate struct {
	ip     net.IPAddr
	score  float64
	weight int
}

func (s *dialIPSelector) ipCandidates(ips []net.IPAddr) ([]ipCandidate, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.stats) == 0 {
		return nil, false
	}

	avg := s.averageIPSpeedLocked("")
	if avg <= 0 {
		return nil, false
	}

	known := false
	candidates := make([]ipCandidate, 0, len(ips))
	for _, ip := range ips {
		key := ipAddrKey(ip)
		stat := s.stats[key]
		score := avg
		if stat != nil && stat.samples > 0 && stat.emaBps > 0 {
			known = true
			score = stat.emaBps
			if stat.slowStreak > 0 {
				score /= 1 + float64(stat.slowStreak)
			}
			if stat.failureStreak > 0 {
				score /= 1 + float64(stat.failureStreak)
			}
		}
		weight := max(int(score/avg*ipQualityUnknownWeight), 1)
		if weight > ipQualityMaxWeight {
			weight = ipQualityMaxWeight
		}
		candidates = append(candidates, ipCandidate{ip: ip, score: score, weight: weight})
	}
	return candidates, known
}

func (s *dialIPSelector) recordIP(key string, bytes int64, elapsed time.Duration, err error) {
	if s == nil || key == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stats == nil {
		s.stats = make(map[string]*ipQuality)
	}
	stat := s.stats[key]
	if stat == nil {
		stat = &ipQuality{}
		s.stats[key] = stat
	}

	if err != nil && bytes <= 0 {
		stat.failureStreak++
		if stat.failureStreak >= ipQualityFailureThreshold {
			stat.quarantinedUntil = time.Now().Add(ipQualityQuarantineDuration)
		}
		return
	}
	if elapsed < ipQualityMinSampleDuration || bytes < ipQualityMinSampleBytes {
		return
	}

	speed := float64(bytes) / elapsed.Seconds()
	if speed <= 0 {
		return
	}
	if stat.emaBps <= 0 {
		stat.emaBps = speed
	} else {
		stat.emaBps = stat.emaBps*(1-ipQualitySmoothFactor) + speed*ipQualitySmoothFactor
	}
	stat.samples++

	avg := s.averageIPSpeedLocked(key)
	if errors.Is(err, errSlowConnection) || (avg > 0 && speed < avg*ipQualitySlowRatio) {
		stat.slowStreak++
	} else {
		stat.slowStreak = 0
		stat.failureStreak = 0
		stat.quarantinedUntil = time.Time{}
	}
	if stat.slowStreak >= ipQualitySlowThreshold {
		stat.quarantinedUntil = time.Now().Add(ipQualityQuarantineDuration)
	}
}

func (s *dialIPSelector) averageIPSpeedLocked(exclude string) float64 {
	var total float64
	var count int
	for key, stat := range s.stats {
		if key == exclude || stat.samples == 0 || stat.emaBps <= 0 {
			continue
		}
		total += stat.emaBps
		count++
	}
	if count == 0 {
		if exclude == "" {
			return 0
		}
		stat := s.stats[exclude]
		if stat != nil {
			return stat.emaBps
		}
		return 0
	}
	return total / float64(count)
}

func splitIPFamilies(ips []net.IPAddr) ([]net.IPAddr, []net.IPAddr, []net.IPAddr) {
	var v4, v6, other []net.IPAddr
	for _, ip := range ips {
		switch {
		case ip.IP.To4() != nil:
			v4 = append(v4, ip)
		case ip.IP.To16() != nil:
			v6 = append(v6, ip)
		default:
			other = append(other, ip)
		}
	}
	return v4, v6, other
}

func rotateIPs(ips []net.IPAddr, next *atomic.Uint64) []net.IPAddr {
	if len(ips) < 2 {
		return ips
	}

	start := int(next.Add(1)-1) % len(ips)
	if start == 0 {
		return ips
	}

	ordered := make([]net.IPAddr, 0, len(ips))
	ordered = append(ordered, ips[start:]...)
	ordered = append(ordered, ips[:start]...)
	return ordered
}

func appendFamilies(groups ...[]net.IPAddr) []net.IPAddr {
	total := 0
	for _, group := range groups {
		total += len(group)
	}
	ordered := make([]net.IPAddr, 0, total)
	for _, group := range groups {
		ordered = append(ordered, group...)
	}
	return ordered
}

func appendInterleaved(first []net.IPAddr, second []net.IPAddr, rest []net.IPAddr) []net.IPAddr {
	ordered := make([]net.IPAddr, 0, len(first)+len(second)+len(rest))
	for i := 0; i < len(first) || i < len(second); i++ {
		if i < len(first) {
			ordered = append(ordered, first[i])
		}
		if i < len(second) {
			ordered = append(ordered, second[i])
		}
	}
	return append(ordered, rest...)
}

func ipAddrKey(ip net.IPAddr) string {
	key := ip.IP.String()
	if ip.Zone != "" {
		key += "%" + ip.Zone
	}
	return key
}

func remoteAddrIPKey(addr net.Addr) string {
	switch a := addr.(type) {
	case *net.TCPAddr:
		return ipAddrKey(net.IPAddr{IP: a.IP, Zone: a.Zone})
	case *net.UDPAddr:
		return ipAddrKey(net.IPAddr{IP: a.IP, Zone: a.Zone})
	default:
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			return ""
		}
		return host
	}
}
