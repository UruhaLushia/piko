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
