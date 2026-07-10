package dialer

import (
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Strategy int

const (
	StrategySequential Strategy = iota
	StrategyRoundRobin
	StrategyFastest
)

type AddressFamily int

const (
	FamilyAuto AddressFamily = iota
	FamilyIPv4
	FamilyIPv6
	FamilyPreferIPv4
	FamilyPreferIPv6
)

type Selector struct {
	strategy     Strategy
	family       AddressFamily
	next         atomic.Uint64
	nextWeighted atomic.Uint64
	next4        atomic.Uint64
	next6        atomic.Uint64
	nextAny      atomic.Uint64

	mu    sync.Mutex
	stats map[string]*ipQuality
}

func NewSelector(strategy Strategy, family AddressFamily) *Selector {
	return &Selector{strategy: strategy, family: family}
}

func (s *Selector) order(ips []net.IPAddr) []net.IPAddr {
	if s == nil || len(ips) < 2 {
		return ips
	}

	switch s.strategy {
	case StrategyRoundRobin:
		return s.roundRobinOrder(ips)
	case StrategySequential:
		return s.sequentialOrder(ips)
	default:
		return s.sequentialOrder(ips)
	}
}

func (s *Selector) sequentialOrder(ips []net.IPAddr) []net.IPAddr {
	ips = s.availableIPs(ips)
	switch s.family {
	case FamilyPreferIPv4:
		v4, v6, other := splitIPFamilies(ips)
		return s.weightedOrder(appendFamilies(v4, other, v6))
	case FamilyPreferIPv6:
		v4, v6, other := splitIPFamilies(ips)
		return s.weightedOrder(appendFamilies(v6, other, v4))
	default:
		return s.weightedOrder(ips)
	}
}

func (s *Selector) roundRobinOrder(ips []net.IPAddr) []net.IPAddr {
	ips = s.availableIPs(ips)
	v4, v6, other := splitIPFamilies(ips)
	v4 = rotateIPs(v4, &s.next4)
	v6 = rotateIPs(v6, &s.next6)
	other = rotateIPs(other, &s.nextAny)

	switch {
	case len(v4) > 0 && len(v6) > 0:
		if s.family == FamilyPreferIPv4 {
			return s.weightedOrder(appendInterleaved(v4, v6, other))
		}
		if s.family == FamilyPreferIPv6 {
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

func (s *Selector) availableIPs(ips []net.IPAddr) []net.IPAddr {
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
		if stat == nil || !stat.quarantinedUntil.After(now) {
			available = append(available, ip)
		}
	}
	if len(available) == 0 {
		return ips
	}
	return available
}

func (s *Selector) weightedOrder(ips []net.IPAddr) []net.IPAddr {
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

func (s *Selector) ipCandidates(ips []net.IPAddr) ([]ipCandidate, bool) {
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
		weight := min(max(int(score/avg*ipQualityUnknownWeight), 1), ipQualityMaxWeight)
		candidates = append(candidates, ipCandidate{ip: ip, score: score, weight: weight})
	}
	return candidates, known
}
