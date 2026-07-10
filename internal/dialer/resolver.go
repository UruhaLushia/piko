package dialer

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const defaultDNSCacheTTL = 5 * time.Minute

type Resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

type resolverFunc func(context.Context, string) ([]net.IPAddr, error)

func (f resolverFunc) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return f(ctx, host)
}

type cachedResolver struct {
	resolver Resolver
	ttl      time.Duration
	group    singleflight.Group

	mu      sync.RWMutex
	entries map[string]dnsCacheEntry
}

type dnsCacheEntry struct {
	ips       []net.IPAddr
	expiresAt time.Time
}

func PrepareResolver(resolver Resolver) Resolver {
	if resolver == nil {
		resolver = resolverFunc(net.DefaultResolver.LookupIPAddr)
	}
	if cached, ok := resolver.(*cachedResolver); ok {
		return cached
	}
	return &cachedResolver{
		resolver: resolver,
		ttl:      defaultDNSCacheTTL,
		entries:  make(map[string]dnsCacheEntry),
	}
}

func (r *cachedResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IPAddr{{IP: ip}}, nil
	}
	if r.ttl <= 0 {
		return r.resolver.LookupIPAddr(ctx, host)
	}

	key := dnsCacheKey(host)
	now := time.Now()
	r.mu.RLock()
	entry, ok := r.entries[key]
	r.mu.RUnlock()
	if ok && now.Before(entry.expiresAt) {
		return cloneIPAddrs(entry.ips), nil
	}

	value, err, _ := r.group.Do(key, func() (any, error) {
		now := time.Now()
		r.mu.RLock()
		entry, ok := r.entries[key]
		r.mu.RUnlock()
		if ok && now.Before(entry.expiresAt) {
			return cloneIPAddrs(entry.ips), nil
		}

		ips, err := r.resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		ips = cloneIPAddrs(ips)
		r.mu.Lock()
		r.entries[key] = dnsCacheEntry{
			ips:       ips,
			expiresAt: now.Add(r.ttl),
		}
		r.mu.Unlock()
		return cloneIPAddrs(ips), nil
	})
	if err != nil {
		return nil, err
	}
	return cloneIPAddrs(value.([]net.IPAddr)), nil
}

func dnsCacheKey(host string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
}

func cloneIPAddrs(ips []net.IPAddr) []net.IPAddr {
	if len(ips) == 0 {
		return nil
	}
	cloned := make([]net.IPAddr, len(ips))
	for i, ip := range ips {
		cloned[i] = ip
		cloned[i].IP = append(net.IP(nil), ip.IP...)
	}
	return cloned
}
