package dialer

import (
	"context"
	"net"
	"sync/atomic"
)

func NewContext(base *net.Dialer, resolver Resolver, selector *Selector) func(context.Context, string, string) (net.Conn, error) {
	if selector == nil {
		selector = NewSelector(StrategyRoundRobin, FamilyAuto)
	}
	resolver = PrepareResolver(resolver)
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || isIPLiteral(host) {
			return base.DialContext(ctx, network, address)
		}

		ips, err := resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		ips = filterIPsForNetwork(network, ips)
		ips = filterIPsForAddressFamily(selector.family, ips)
		if len(ips) == 0 {
			return nil, &net.DNSError{Err: "no suitable address", Name: host}
		}
		ips = selector.order(ips)
		if selector.strategy == StrategyFastest {
			return dialFastestIP(ctx, base, selector, network, port, ips)
		}

		var lastErr error
		for _, ip := range ips {
			conn, err := base.DialContext(ctx, network, joinIPPort(ip, port))
			if err == nil {
				return conn, nil
			}
			if ctx.Err() == nil {
				selector.RecordIP(ipAddrKey(ip), 0, 0, err)
			}
			lastErr = err
		}
		return nil, lastErr
	}
}

type dialResult struct {
	conn net.Conn
	err  error
}

func dialFastestIP(ctx context.Context, base *net.Dialer, selector *Selector, network string, port string, ips []net.IPAddr) (net.Conn, error) {
	if len(ips) == 1 {
		conn, err := base.DialContext(ctx, network, joinIPPort(ips[0], port))
		if err != nil && ctx.Err() == nil {
			selector.RecordIP(ipAddrKey(ips[0]), 0, 0, err)
		}
		return conn, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan dialResult, len(ips))
	var won atomic.Bool
	for _, ip := range ips {
		address := joinIPPort(ip, port)
		go func() {
			conn, err := base.DialContext(ctx, network, address)
			if err != nil && ctx.Err() == nil {
				selector.RecordIP(ipAddrKey(ip), 0, 0, err)
			}
			if err == nil {
				if !won.CompareAndSwap(false, true) {
					_ = conn.Close()
					return
				}
				cancel()
			}
			results <- dialResult{conn: conn, err: err}
		}()
	}

	var lastErr error
	for range ips {
		result := <-results
		if result.err == nil {
			return result.conn, nil
		}
		lastErr = result.err
	}
	return nil, lastErr
}
