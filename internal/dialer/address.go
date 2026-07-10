package dialer

import (
	"net"
	"strings"
	"sync/atomic"
)

func joinIPPort(ip net.IPAddr, port string) string {
	host := ip.IP.String()
	if ip.Zone != "" {
		host += "%" + ip.Zone
	}
	return net.JoinHostPort(host, port)
}

func isIPLiteral(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	if address, _, ok := strings.Cut(host, "%"); ok {
		return net.ParseIP(address) != nil
	}
	return false
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

func filterIPsForAddressFamily(family AddressFamily, ips []net.IPAddr) []net.IPAddr {
	switch family {
	case FamilyIPv4:
		filtered := ips[:0]
		for _, ip := range ips {
			if ip.IP.To4() != nil {
				filtered = append(filtered, ip)
			}
		}
		return filtered
	case FamilyIPv6:
		filtered := ips[:0]
		for _, ip := range ips {
			if ip.IP.To4() == nil && ip.IP.To16() != nil {
				filtered = append(filtered, ip)
			}
		}
		return filtered
	default:
		return ips
	}
}

func splitIPFamilies(ips []net.IPAddr) ([]net.IPAddr, []net.IPAddr, []net.IPAddr) {
	var v4, v6, other []net.IPAddr
	for _, ip := range ips {
		switch {
		case ip.IP.To4() != nil:
			v4 = append(v4, ip)
		case ip.IP.To16() != nil:
			v6 = append(v6, ip)
		default:
			other = append(other, ip)
		}
	}
	return v4, v6, other
}

func rotateIPs(ips []net.IPAddr, next *atomic.Uint64) []net.IPAddr {
	if len(ips) < 2 {
		return ips
	}

	start := int(next.Add(1)-1) % len(ips)
	if start == 0 {
		return ips
	}

	ordered := make([]net.IPAddr, 0, len(ips))
	ordered = append(ordered, ips[start:]...)
	ordered = append(ordered, ips[:start]...)
	return ordered
}

func appendFamilies(groups ...[]net.IPAddr) []net.IPAddr {
	total := 0
	for _, group := range groups {
		total += len(group)
	}
	ordered := make([]net.IPAddr, 0, total)
	for _, group := range groups {
		ordered = append(ordered, group...)
	}
	return ordered
}

func appendInterleaved(first []net.IPAddr, second []net.IPAddr, rest []net.IPAddr) []net.IPAddr {
	ordered := make([]net.IPAddr, 0, len(first)+len(second)+len(rest))
	for i := 0; i < len(first) || i < len(second); i++ {
		if i < len(first) {
			ordered = append(ordered, first[i])
		}
		if i < len(second) {
			ordered = append(ordered, second[i])
		}
	}
	return append(ordered, rest...)
}

func ipAddrKey(ip net.IPAddr) string {
	key := ip.IP.String()
	if ip.Zone != "" {
		key += "%" + ip.Zone
	}
	return key
}

func RemoteAddrIPKey(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	switch a := addr.(type) {
	case *net.TCPAddr:
		return ipAddrKey(net.IPAddr{IP: a.IP, Zone: a.Zone})
	case *net.UDPAddr:
		return ipAddrKey(net.IPAddr{IP: a.IP, Zone: a.Zone})
	default:
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			return ""
		}
		return host
	}
}
