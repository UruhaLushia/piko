package piko

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type HTTPOptions struct {
	Timeout         time.Duration
	MaxConnsPerHost int
	Protocol        Protocol
	Proxy           string
	ProxyFunc       func(*http.Request) (*url.URL, error)
	Resolver        Resolver
}

func DefaultHTTPOptions() HTTPOptions {
	return HTTPOptions{
		Timeout:         DefaultTimeout,
		MaxConnsPerHost: DefaultConnections,
		Protocol:        ProtocolAuto,
	}
}

func NewHTTPClient(opts HTTPOptions) (*http.Client, error) {
	transport, err := NewTransport(opts)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}

func NewTransport(opts HTTPOptions) (*http.Transport, error) {
	opts = opts.normalize()
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
		}, opts.Resolver),
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

func newHTTPClient(timeout time.Duration, maxConnsPerHost int, protocol Protocol, proxy string, proxyFunc func(*http.Request) (*url.URL, error), resolver Resolver) (*http.Client, error) {
	return NewHTTPClient(HTTPOptions{
		Timeout:         timeout,
		MaxConnsPerHost: maxConnsPerHost,
		Protocol:        protocol,
		Proxy:           proxy,
		ProxyFunc:       proxyFunc,
		Resolver:        resolver,
	})
}

func newHTTPClients(count int, timeout time.Duration, protocol Protocol, proxy string, proxyFunc func(*http.Request) (*url.URL, error), resolver Resolver) ([]*http.Client, error) {
	if count < 1 {
		count = 1
	}
	clients := make([]*http.Client, 0, count)
	for range count {
		client, err := newHTTPClient(timeout, 1, protocol, proxy, proxyFunc, resolver)
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
	case "", "env", "environment":
		return http.ProxyFromEnvironment, nil
	case "direct", "none", "off":
		return nil, nil
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

func newDialContext(dialer *net.Dialer, resolver Resolver) func(context.Context, string, string) (net.Conn, error) {
	if resolver == nil {
		return dialer.DialContext
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
