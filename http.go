package piko

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

type HTTPOptions struct {
	Timeout            time.Duration
	MaxConnsPerHost    int
	Protocol           Protocol
	ConnectionStrategy ConnectionStrategy
	Proxy              string
	ProxyFunc          func(*http.Request) (*url.URL, error)
	Resolver           Resolver
}

func DefaultHTTPOptions() HTTPOptions {
	return HTTPOptions{
		Timeout:         DefaultTimeout,
		MaxConnsPerHost: DefaultConnections,
		Protocol:        ProtocolAuto,
	}
}

func NewHTTPClient(opts HTTPOptions) (*http.Client, error) {
	return newHTTPClientFromOptions(opts, nil)
}

func newHTTPClientFromOptions(opts HTTPOptions, selector *dialIPSelector) (*http.Client, error) {
	transport, err := newTransport(opts, selector)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}

func NewTransport(opts HTTPOptions) (*http.Transport, error) {
	return newTransport(opts, nil)
}

func newTransport(opts HTTPOptions, selector *dialIPSelector) (*http.Transport, error) {
	opts = opts.normalize()
	if selector == nil {
		selector = newDialIPSelector(opts.ConnectionStrategy)
	}

	proxy, err := proxyFromOptions(opts)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		Proxy: proxy,
		DialContext: newDialContext(&net.Dialer{
			Timeout:       opts.Timeout,
			KeepAlive:     30 * time.Second,
			FallbackDelay: 100 * time.Millisecond,
		}, opts.Resolver, selector),
		MaxIdleConns:          opts.MaxConnsPerHost,
		MaxIdleConnsPerHost:   opts.MaxConnsPerHost,
		MaxConnsPerHost:       opts.MaxConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   opts.Timeout,
		ResponseHeaderTimeout: opts.Timeout,
		ForceAttemptHTTP2:     opts.Protocol == ProtocolAuto,
	}
	configureTransportProtocols(transport, opts.Protocol)
	return transport, nil
}

func newHTTPClient(timeout time.Duration, maxConnsPerHost int, protocol Protocol, strategy ConnectionStrategy, proxy string, proxyFunc func(*http.Request) (*url.URL, error), resolver Resolver, selector *dialIPSelector) (*http.Client, error) {
	return newHTTPClientFromOptions(HTTPOptions{
		Timeout:            timeout,
		MaxConnsPerHost:    maxConnsPerHost,
		Protocol:           protocol,
		ConnectionStrategy: strategy,
		Proxy:              proxy,
		ProxyFunc:          proxyFunc,
		Resolver:           resolver,
	}, selector)
}

func newHTTPClients(count int, timeout time.Duration, protocol Protocol, strategy ConnectionStrategy, proxy string, proxyFunc func(*http.Request) (*url.URL, error), resolver Resolver) ([]*http.Client, error) {
	if count < 1 {
		count = 1
	}
	selector := newDialIPSelector(strategy)
	clients := make([]*http.Client, 0, count)
	for range count {
		client, err := newHTTPClient(timeout, 1, protocol, strategy, proxy, proxyFunc, resolver, selector)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	return clients, nil
}

func (o HTTPOptions) normalize() HTTPOptions {
	defaults := DefaultHTTPOptions()
	if o.Timeout <= 0 {
		o.Timeout = defaults.Timeout
	}
	if o.MaxConnsPerHost < 1 {
		o.MaxConnsPerHost = 1
	}
	return o
}

func proxyFromOptions(opts HTTPOptions) (func(*http.Request) (*url.URL, error), error) {
	if opts.ProxyFunc != nil {
		return opts.ProxyFunc, nil
	}
	proxy := strings.TrimSpace(opts.Proxy)
	switch strings.ToLower(proxy) {
	case "", "direct", "none", "off":
		return nil, nil
	case "env", "environment":
		return http.ProxyFromEnvironment, nil
	}
	proxyURL, err := parseProxyURL(proxy)
	if err != nil {
		return nil, err
	}
	return http.ProxyURL(proxyURL), nil
}

func parseProxyURL(value string) (*url.URL, error) {
	if !strings.Contains(value, "://") {
		value = "http://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy %q: %w", value, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid proxy %q", value)
	}
	return parsed, nil
}

func newDialContext(dialer *net.Dialer, resolver Resolver, selector *dialIPSelector) func(context.Context, string, string) (net.Conn, error) {
	if selector == nil {
		selector = newDialIPSelector(ConnectionStrategySequential)
	}
	if resolver == nil {
		if selector.strategy == ConnectionStrategySequential {
			return dialer.DialContext
		}
		resolver = resolverFunc(net.DefaultResolver.LookupIPAddr)
	}
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || net.ParseIP(host) != nil {
			return dialer.DialContext(ctx, network, address)
		}

		ips, err := resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		ips = filterIPsForNetwork(network, ips)
		if len(ips) == 0 {
			return nil, &net.DNSError{Err: "no suitable address", Name: host}
		}
		if selector.strategy == ConnectionStrategyFastest {
			return dialFastestIP(ctx, dialer, network, port, ips)
		}
		ips = selector.order(ips)

		var lastErr error
		for _, ip := range ips {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
			if err == nil {
				return conn, nil
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

func dialFastestIP(ctx context.Context, dialer *net.Dialer, network string, port string, ips []net.IPAddr) (net.Conn, error) {
	if len(ips) == 1 {
		return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan dialResult, len(ips))
	var won atomic.Bool
	for _, ip := range ips {
		address := net.JoinHostPort(ip.IP.String(), port)
		go func() {
			conn, err := dialer.DialContext(ctx, network, address)
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

func filterIPsForNetwork(network string, ips []net.IPAddr) []net.IPAddr {
	filtered := ips[:0]
	for _, ip := range ips {
		switch {
		case strings.HasSuffix(network, "4") && ip.IP.To4() == nil:
			continue
		case strings.HasSuffix(network, "6") && ip.IP.To4() != nil:
			continue
		default:
			filtered = append(filtered, ip)
		}
	}
	return filtered
}

func configureTransportProtocols(transport *http.Transport, protocol Protocol) {
	if protocol == ProtocolAuto {
		return
	}

	protocols := new(http.Protocols)
	switch protocol {
	case ProtocolHTTP1:
		protocols.SetHTTP1(true)
	case ProtocolHTTP2:
		protocols.SetHTTP2(true)
	case ProtocolH2C:
		protocols.SetUnencryptedHTTP2(true)
	default:
		return
	}
	transport.Protocols = protocols
}
