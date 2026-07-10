package dns

import (
	"context"
	"fmt"
	"net"

	mdns "github.com/miekg/dns"
)

type queryResult struct {
	qtype uint16
	ips   []net.IP
	err   error
}

func lookupIPAddr(ctx context.Context, host string, exchange func(context.Context, *mdns.Msg) (*mdns.Msg, error)) ([]net.IPAddr, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IPAddr{{IP: ip}}, nil
	}

	qtypes := [...]uint16{mdns.TypeA, mdns.TypeAAAA}
	results := make(chan queryResult, len(qtypes))
	for _, qtype := range qtypes {
		go func() {
			resp, err := exchange(ctx, newQuery(host, qtype))
			if err != nil {
				results <- queryResult{qtype: qtype, err: err}
				return
			}
			ips, err := ipsFromResponse(resp, host, qtype)
			results <- queryResult{qtype: qtype, ips: ips, err: err}
		}()
	}

	answers := make(map[uint16][]net.IP, len(qtypes))
	var lastErr error
	for range qtypes {
		result := <-results
		if result.err != nil {
			lastErr = result.err
			continue
		}
		answers[result.qtype] = result.ips
	}

	seen := make(map[string]struct{})
	var resolved []net.IPAddr
	for _, qtype := range qtypes {
		for _, ip := range answers[qtype] {
			key := ip.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			resolved = append(resolved, net.IPAddr{IP: ip})
		}
	}
	if len(resolved) > 0 {
		return resolved, nil
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
	if resp.Rcode == mdns.RcodeNameError {
		return nil, &net.DNSError{Err: "no such host", Name: host}
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
	return ips, nil
}
