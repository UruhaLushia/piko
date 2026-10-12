package piko

import (
	"net/http"
	"net/url"
	"time"
)

const (
	DefaultUserAgent    = "piko/1.0"
	DefaultConnections  = 16
	DefaultRetries      = 3
	DefaultTimeout      = 30 * time.Second
	DefaultStallTimeout = 15 * time.Second
	DefaultPartSize     = 4 * 1024 * 1024
)

type Options struct {
	Output              string
	Offset              int64
	Length              int64
	Connections         int
	Retries             int
	Force               bool
	Resume              bool
	HedgeEnabled        bool
	ConcurrencyStrategy ConcurrencyStrategy
	PartSize            int64
	Timeout             time.Duration
	StallTimeout        time.Duration
	UserAgent           string
	Headers             http.Header
	Sources             []Source
	Protocol            Protocol
	ConnectionStrategy  ConnectionStrategy
	AddressFamily       AddressFamily
	Proxy               string
	ProxyFunc           func(*http.Request) (*url.URL, error)
	Resolver            Resolver
	HTTPClient          *http.Client
	HTTPClients         []*http.Client
	Started             func(Result)
	Progress            func(Progress)
}

// Source is an alternate URL and its request headers for a download.
// An empty Sources list keeps the original single-URL behavior.
type Source struct {
	URL     string
	Headers http.Header
}

type Progress struct {
	Bytes   int64
	Total   int64
	Resumed int64
	Done    bool
}

type Result struct {
	Output      string
	Offset      int64
	Size        int64
	TotalSize   int64
	Rangeable   bool
	Discarded   bool
	FinalURL    string
	Connections int
	Parallel    bool
	Segmented   bool
	PartSize    int64
	SourceStats []SourceStat
}

type SourceStat struct {
	URL      string
	Bytes    int64
	Attempts int
	Outcome  string
	// Rate is the measured throughput in bytes per second per request.
	Rate float64
	TTFB time.Duration
}

func DefaultOptions() Options {
	return Options{
		Connections:         DefaultConnections,
		Retries:             DefaultRetries,
		PartSize:            DefaultPartSize,
		Timeout:             DefaultTimeout,
		StallTimeout:        DefaultStallTimeout,
		UserAgent:           DefaultUserAgent,
		Protocol:            ProtocolAuto,
		ConcurrencyStrategy: ConcurrencyStrategyFixed,
		ConnectionStrategy:  ConnectionStrategyRoundRobin,
		AddressFamily:       AddressFamilyAuto,
	}
}

func (o Options) normalize() Options {
	defaults := DefaultOptions()
	if o.Connections <= 0 {
		o.Connections = defaults.Connections
	}
	if o.Retries < 0 {
		o.Retries = 0
	}
	if o.PartSize <= 0 {
		o.PartSize = defaults.PartSize
	}
	if o.PartSize < 1024*1024 {
		o.PartSize = 1024 * 1024
	}
	if o.Timeout <= 0 {
		o.Timeout = defaults.Timeout
	}
	if o.StallTimeout < 0 {
		o.StallTimeout = 0
	}
	if o.UserAgent == "" {
		o.UserAgent = defaults.UserAgent
	}
	o.Sources = append([]Source(nil), o.Sources...)
	for i := range o.Sources {
		o.Sources[i].Headers = cloneHeader(o.Sources[i].Headers)
	}
	if o.ConcurrencyStrategy == ConcurrencyStrategyDefault {
		o.ConcurrencyStrategy = defaults.ConcurrencyStrategy
	}
	if o.ConnectionStrategy == ConnectionStrategyDefault {
		o.ConnectionStrategy = defaults.ConnectionStrategy
	}
	return o
}
