package piko

import "github.com/UruhaLushia/piko/internal/dns"

// ParseResolver parses a DNS resolver string for Options.Resolver.
func ParseResolver(value string) (Resolver, error) {
	return dns.ParseResolver(value)
}
