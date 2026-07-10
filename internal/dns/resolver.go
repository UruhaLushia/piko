package dns

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

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

func ParseResolver(values ...string) (Resolver, error) {
	values = compactResolverValues(values)
	if len(values) == 0 {
		return nil, nil
	}

	resolvers := make([]Resolver, 0, len(values))
	for _, value := range values {
		resolver, system, err := parseSingleResolver(value)
		if err != nil {
			return nil, err
		}
		if resolver != nil {
			resolvers = append(resolvers, resolver)
			continue
		}
		if system && len(values) > 1 {
			resolvers = append(resolvers, NewSystemResolver())
		}
	}
	return NewMultiResolver(resolvers...), nil
}

func parseSingleResolver(value string) (Resolver, bool, error) {
	text := strings.TrimSpace(value)
	switch strings.ToLower(text) {
	case "", "system", "default", "env":
		return nil, true, nil
	}

	if !strings.Contains(text, "://") {
		return NewDNSResolver("udp", text), false, nil
	}

	u, err := url.Parse(text)
	if err != nil {
		return nil, false, err
	}
	switch strings.ToLower(u.Scheme) {
	case "dns", "udp":
		return NewDNSResolver("udp", resolverAddress(u, text)), false, nil
	case "tcp":
		return NewDNSResolver("tcp", resolverAddress(u, text)), false, nil
	case "tls", "dot":
		serverName := u.Query().Get("name")
		if serverName == "" {
			serverName = u.Query().Get("server_name")
		}
		return NewDoTResolver(resolverAddress(u, text), serverName, 0), false, nil
	case "https":
		resolver, err := NewDoHResolver(text, nil)
		return resolver, false, err
	case "doh":
		endpoint := "https://" + strings.TrimPrefix(text, "doh://")
		resolver, err := NewDoHResolver(endpoint, nil)
		return resolver, false, err
	default:
		return nil, false, fmt.Errorf("unknown resolver %q", value)
	}
}

func NewSystemResolver() Resolver {
	return resolverFunc(net.DefaultResolver.LookupIPAddr)
}

func NewMultiResolver(resolvers ...Resolver) Resolver {
	compacted := make([]Resolver, 0, len(resolvers))
	for _, resolver := range resolvers {
		if resolver != nil {
			compacted = append(compacted, resolver)
		}
	}
	switch len(compacted) {
	case 0:
		return nil
	case 1:
		return compacted[0]
	default:
		return &multiResolver{resolvers: compacted}
	}
}

type multiResolver struct {
	resolvers []Resolver
}

type resolverResult struct {
	ips []net.IPAddr
	err error
}

func (r *multiResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IPAddr{{IP: ip}}, nil
	}
	if len(r.resolvers) == 0 {
		return nil, &net.DNSError{Err: "no resolver configured", Name: host}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan resolverResult, len(r.resolvers))
	for _, resolver := range r.resolvers {
		go func() {
			ips, err := resolver.LookupIPAddr(ctx, host)
			select {
			case results <- resolverResult{ips: ips, err: err}:
			case <-ctx.Done():
			}
		}()
	}

	var lastErr error
	for range r.resolvers {
		select {
		case result := <-results:
			if len(result.ips) > 0 {
				cancel()
				return result.ips, nil
			}
			if result.err != nil {
				lastErr = result.err
			}
		case <-ctx.Done():
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, ctx.Err()
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, &net.DNSError{Err: "no such host", Name: host}
}

func splitResolverValues(value string) []string {
	return strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\r'
	})
}

func compactResolverValues(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		for _, part := range splitResolverValues(value) {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}
