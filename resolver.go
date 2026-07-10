package piko

import (
	"context"
	"net"
)

// Resolver resolves host names for HTTP connections.
type Resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}
