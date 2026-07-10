package piko

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/UruhaLushia/piko/internal/dialer"
)

type HTTPOptions struct {
	Timeout            time.Duration
	MaxConnsPerHost    int
	Protocol           Protocol
	ConnectionStrategy ConnectionStrategy
	AddressFamily      AddressFamily
	Proxy              string
	ProxyFunc          func(*http.Request) (*url.URL, error)
	Resolver           Resolver
}

func DefaultHTTPOptions() HTTPOptions {
	return HTTPOptions{
		Timeout:            DefaultTimeout,
		MaxConnsPerHost:    DefaultConnections,
		Protocol:           ProtocolAuto,
		ConnectionStrategy: ConnectionStrategyRoundRobin,
		AddressFamily:      AddressFamilyAuto,
	}
}

func NewHTTPClient(opts HTTPOptions) (*http.Client, error) {
	return newHTTPClientFromOptions(opts, nil)
}

func newHTTPClientFromOptions(opts HTTPOptions, selector *dialer.Selector) (*http.Client, error) {
	transport, err := newTransport(opts, selector)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}

func NewTransport(opts HTTPOptions) (*http.Transport, error) {
	return newTransport(opts, nil)
}

func newTransport(opts HTTPOptions, selector *dialer.Selector) (*http.Transport, error) {
	opts = opts.normalize()
	if selector == nil {
		selector = newDialSelector(opts.ConnectionStrategy, opts.AddressFamily)
	}

	proxy, err := proxyFromOptions(opts)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		Proxy: proxy,
		DialContext: dialer.NewContext(&net.Dialer{
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

func newHTTPClients(count int, opts HTTPOptions) ([]*http.Client, *dialer.Selector, error) {
	if count < 1 {
		count = 1
	}
	opts = opts.normalize()
	opts.MaxConnsPerHost = 1
	opts.Resolver = dialer.PrepareResolver(opts.Resolver)
	selector := newDialSelector(opts.ConnectionStrategy, opts.AddressFamily)
	clients := make([]*http.Client, 0, count)
	for range count {
		client, err := newHTTPClientFromOptions(opts, selector)
		if err != nil {
			return nil, nil, err
		}
		clients = append(clients, client)
	}
	return clients, selector, nil
}

func (o HTTPOptions) normalize() HTTPOptions {
	defaults := DefaultHTTPOptions()
	if o.Timeout <= 0 {
		o.Timeout = defaults.Timeout
	}
	if o.MaxConnsPerHost < 1 {
		o.MaxConnsPerHost = 1
	}
	if o.ConnectionStrategy == ConnectionStrategyDefault {
		o.ConnectionStrategy = defaults.ConnectionStrategy
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

func newDialSelector(strategy ConnectionStrategy, family AddressFamily) *dialer.Selector {
	return dialer.NewSelector(toDialStrategy(strategy), toDialAddressFamily(family))
}

func toDialStrategy(strategy ConnectionStrategy) dialer.Strategy {
	switch strategy {
	case ConnectionStrategySequential:
		return dialer.StrategySequential
	case ConnectionStrategyFastest:
		return dialer.StrategyFastest
	default:
		return dialer.StrategyRoundRobin
	}
}

func toDialAddressFamily(family AddressFamily) dialer.AddressFamily {
	switch family {
	case AddressFamilyIPv4:
		return dialer.FamilyIPv4
	case AddressFamilyIPv6:
		return dialer.FamilyIPv6
	case AddressFamilyPreferIPv4:
		return dialer.FamilyPreferIPv4
	case AddressFamilyPreferIPv6:
		return dialer.FamilyPreferIPv6
	default:
		return dialer.FamilyAuto
	}
}
