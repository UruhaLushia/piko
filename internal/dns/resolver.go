package dns

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	mdns "github.com/miekg/dns"
)

const defaultTimeout = 30 * time.Second

type Resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

type ResolverKind string

const (
	ResolverSystem ResolverKind = "system"
	ResolverUDP    ResolverKind = "udp"
	ResolverTCP    ResolverKind = "tcp"
	ResolverDoT    ResolverKind = "dot"
	ResolverDoH    ResolverKind = "doh"
)

type ResolverOptions struct {
	Kind       ResolverKind
	Address    string
	Endpoint   string
	ServerName string
	Timeout    time.Duration
	HTTPClient *http.Client
}

type resolverFunc func(context.Context, string) ([]net.IPAddr, error)

func (f resolverFunc) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return f(ctx, host)
}

func NewResolver(opts ResolverOptions) (Resolver, error) {
	switch opts.Kind {
	case "", ResolverSystem:
		return NewSystemResolver(), nil
	case ResolverUDP:
		return NewDNSResolver("udp", opts.Address), nil
	case ResolverTCP:
		return NewDNSResolver("tcp", opts.Address), nil
	case ResolverDoT:
		return NewDoTResolver(opts.Address, opts.ServerName, opts.Timeout), nil
	case ResolverDoH:
		return NewDoHResolver(opts.Endpoint, opts.HTTPClient)
	default:
		return nil, fmt.Errorf("unknown resolver kind %q", opts.Kind)
	}
}

func ParseResolver(value string) (Resolver, error) {
	text := strings.TrimSpace(value)
	switch strings.ToLower(text) {
	case "", "system", "default", "env":
		return nil, nil
	}

	if !strings.Contains(text, "://") {
		return NewDNSResolver("udp", text), nil
	}

	u, err := url.Parse(text)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(u.Scheme) {
	case "dns", "udp":
		return NewDNSResolver("udp", resolverAddress(u, text)), nil
	case "tcp":
		return NewDNSResolver("tcp", resolverAddress(u, text)), nil
	case "tls", "dot":
		serverName := u.Query().Get("name")
		if serverName == "" {
			serverName = u.Query().Get("server_name")
		}
		return NewDoTResolver(resolverAddress(u, text), serverName, 0), nil
	case "https":
		return NewDoHResolver(text, nil)
	case "doh":
		endpoint := "https://" + strings.TrimPrefix(text, "doh://")
		return NewDoHResolver(endpoint, nil)
	default:
		return nil, fmt.Errorf("unknown resolver %q", value)
	}
}

func NewSystemResolver() Resolver {
	return resolverFunc(net.DefaultResolver.LookupIPAddr)
}

func NewDNSResolver(network string, address string) Resolver {
	if network == "" || network == "dns" {
		network = "udp"
	}
	return &wireResolver{
		client:  newDNSClient(network, "", 0),
		address: withDefaultPort(address, "53"),
	}
}

func NewDoTResolver(address string, serverName string, timeout time.Duration) Resolver {
	address = withDefaultPort(address, "853")
	if serverName == "" {
		serverName = serverNameFromAddress(address)
	}
	return &wireResolver{
		client:  newDNSClient("tcp-tls", serverName, timeout),
		address: address,
	}
}

type wireResolver struct {
	client  *mdns.Client
	address string
}

func (r *wireResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return lookupIPAddr(ctx, host, func(ctx context.Context, msg *mdns.Msg) (*mdns.Msg, error) {
		resp, _, err := r.client.ExchangeContext(ctx, msg, r.address)
		return resp, err
	})
}

func newDNSClient(network string, serverName string, timeout time.Duration) *mdns.Client {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	client := &mdns.Client{
		Net:     network,
		Timeout: timeout,
	}
	if network == "tcp-tls" {
		client.TLSConfig = &tls.Config{ServerName: serverName}
	}
	return client
}

func NewDoHResolver(endpoint string, client *http.Client) (Resolver, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("missing DoH endpoint")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("DoH endpoint must be an https URL")
	}
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}
	return &dohResolver{endpoint: endpoint, client: client}, nil
}

type dohResolver struct {
	endpoint string
	client   *http.Client
}

func (r *dohResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return lookupIPAddr(ctx, host, r.exchange)
}

func (r *dohResolver) exchange(ctx context.Context, msg *mdns.Msg) (*mdns.Msg, error) {
	query, err := msg.Pack()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(query))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/dns-message")
	req.Header.Set("Content-Type", "application/dns-message")

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("DoH query failed: %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return nil, err
	}

	answer := new(mdns.Msg)
	if err := answer.Unpack(body); err != nil {
		return nil, err
	}
	return answer, nil
}

func lookupIPAddr(ctx context.Context, host string, exchange func(context.Context, *mdns.Msg) (*mdns.Msg, error)) ([]net.IPAddr, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IPAddr{{IP: ip}}, nil
	}

	var result []net.IPAddr
	var lastErr error
	seen := map[string]struct{}{}
	for _, qtype := range []uint16{mdns.TypeA, mdns.TypeAAAA} {
		msg := newQuery(host, qtype)
		resp, err := exchange(ctx, msg)
		if err != nil {
			lastErr = err
			continue
		}
		ips, err := ipsFromResponse(resp, host, qtype)
		if err != nil {
			lastErr = err
			continue
		}
		for _, ip := range ips {
			key := ip.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			result = append(result, net.IPAddr{IP: ip})
		}
	}
	if len(result) > 0 {
		return result, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, &net.DNSError{Err: "no such host", Name: host}
}

func newQuery(host string, qtype uint16) *mdns.Msg {
	msg := new(mdns.Msg)
	msg.SetQuestion(mdns.Fqdn(host), qtype)
	msg.RecursionDesired = true
	msg.SetEdns0(1232, false)
	return msg
}

func ipsFromResponse(resp *mdns.Msg, host string, qtype uint16) ([]net.IP, error) {
	if resp == nil {
		return nil, fmt.Errorf("empty dns response")
	}
	if resp.Rcode != mdns.RcodeSuccess {
		return nil, fmt.Errorf("dns response code %s", mdns.RcodeToString[resp.Rcode])
	}

	var ips []net.IP
	for _, answer := range resp.Answer {
		switch record := answer.(type) {
		case *mdns.A:
			if qtype == mdns.TypeA {
				ips = append(ips, record.A)
			}
		case *mdns.AAAA:
			if qtype == mdns.TypeAAAA {
				ips = append(ips, record.AAAA)
			}
		}
	}
	if len(ips) == 0 && resp.Rcode == mdns.RcodeNameError {
		return nil, &net.DNSError{Err: "no such host", Name: host}
	}
	return ips, nil
}

func resolverAddress(u *url.URL, raw string) string {
	if u.Host != "" {
		return u.Host
	}
	return strings.TrimPrefix(raw, u.Scheme+"://")
}

func withDefaultPort(address string, port string) string {
	address = strings.TrimSpace(address)
	if address == "" {
		address = "1.1.1.1"
	}
	if _, _, err := net.SplitHostPort(address); err == nil {
		return address
	}
	if ip := net.ParseIP(strings.Trim(address, "[]")); ip != nil {
		return net.JoinHostPort(ip.String(), port)
	}
	return net.JoinHostPort(address, port)
}

func serverNameFromAddress(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return strings.Trim(address, "[]")
	}
	return strings.Trim(host, "[]")
}
