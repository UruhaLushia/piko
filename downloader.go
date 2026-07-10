package piko

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/UruhaLushia/piko/internal/dialer"
)

const minBytesPerConnection = 5 * 1024 * 1024

type downloader struct {
	client       *http.Client
	clients      []*http.Client
	selector     *dialer.Selector
	url          string
	ua           string
	headers      http.Header
	retries      int
	stallTimeout time.Duration
	progress     func(Progress)
	total        int64

	done atomic.Int64

	speedMu      sync.Mutex
	nextSpeedID  int64
	activeSpeeds map[int64]float64
}

type Client struct {
	opts     Options
	client   *http.Client
	clients  []*http.Client
	selector *dialer.Selector
}

func NewClient(opts Options) (*Client, error) {
	opts = opts.normalize()

	clients := compactHTTPClients(opts.HTTPClients)
	client := opts.HTTPClient
	var selector *dialer.Selector
	switch {
	case client != nil:
		if len(clients) == 0 {
			clients = []*http.Client{client}
		}
	case len(clients) > 0:
		client = clients[0]
	default:
		var err error
		clients, selector, err = newHTTPClients(opts.Connections, HTTPOptions{
			Timeout:            opts.Timeout,
			Protocol:           opts.Protocol,
			ConnectionStrategy: opts.ConnectionStrategy,
			AddressFamily:      opts.AddressFamily,
			Proxy:              opts.Proxy,
			ProxyFunc:          opts.ProxyFunc,
			Resolver:           opts.Resolver,
		})
		if err != nil {
			return nil, err
		}
		client = clients[0]
	}

	opts.Headers = cloneHeader(opts.Headers)
	return &Client{opts: opts, client: client, clients: clients, selector: selector}, nil
}

func (c *Client) Download(ctx context.Context, rawURL string) (Result, error) {
	d := newDownloader(rawURL, c.opts, c.client, c.clients, c.selector)
	return d.run(ctx, c.opts)
}

// DownloadBytes downloads rawURL into memory without creating files.
func (c *Client) DownloadBytes(ctx context.Context, rawURL string) ([]byte, Result, error) {
	d := newDownloader(rawURL, c.opts, c.client, c.clients, c.selector)
	return d.runBytes(ctx, c.opts)
}

// Download downloads rawURL using opts and returns the resolved output details.
func Download(ctx context.Context, rawURL string, opts Options) (Result, error) {
	client, err := NewClient(opts)
	if err != nil {
		return Result{}, err
	}
	return client.Download(ctx, rawURL)
}

// DownloadBytes downloads rawURL into memory without creating files.
func DownloadBytes(ctx context.Context, rawURL string, opts Options) ([]byte, Result, error) {
	client, err := NewClient(opts)
	if err != nil {
		return nil, Result{}, err
	}
	return client.DownloadBytes(ctx, rawURL)
}

func newDownloader(rawURL string, opts Options, client *http.Client, clients []*http.Client, selector *dialer.Selector) *downloader {
	return &downloader{
		client:       client,
		clients:      clients,
		selector:     selector,
		url:          rawURL,
		ua:           opts.UserAgent,
		headers:      opts.Headers,
		retries:      opts.Retries,
		stallTimeout: opts.StallTimeout,
		progress:     opts.Progress,
	}
}

func compactHTTPClients(clients []*http.Client) []*http.Client {
	if len(clients) == 0 {
		return nil
	}
	compacted := make([]*http.Client, 0, len(clients))
	for _, client := range clients {
		if client != nil {
			compacted = append(compacted, client)
		}
	}
	return compacted
}

func cloneHeader(header http.Header) http.Header {
	if len(header) == 0 {
		return nil
	}
	return header.Clone()
}

func (d *downloader) run(ctx context.Context, opts Options) (Result, error) {
	result, err := d.plan(ctx, opts, true)
	if err != nil {
		return Result{}, err
	}

	if !result.Discarded {
		if err := prepareOutput(result.Output, opts.Force); err != nil {
			return Result{}, err
		}
	}

	if opts.Started != nil {
		opts.Started(result)
	}

	d.total = result.Size
	if result.Parallel {
		err = d.downloadParts(ctx, result.Output, result.Size, result.PartSize, result.Connections, opts.Force)
	} else {
		err = d.downloadSingle(ctx, result.Output, result.Size, opts.Force)
	}
	if err != nil {
		return result, err
	}
	return result, nil
}

func (d *downloader) plan(ctx context.Context, opts Options, allowDiscard bool) (Result, error) {
	parsed, err := url.Parse(d.url)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return Result{}, fmt.Errorf("invalid url: %q", d.url)
	}

	info, err := d.inspect(ctx)
	if err != nil {
		return Result{}, err
	}
	if info.finalURL != "" {
		d.url = info.finalURL
		if finalURL, err := url.Parse(info.finalURL); err == nil && finalURL.Scheme != "" && finalURL.Host != "" {
			parsed = finalURL
		}
	}

	output := resolveOutputPath(opts.Output, parsed, info.suggested)
	discard := allowDiscard && IsNullOutput(output)

	connections := opts.Connections
	parallel := connections > 1 && info.rangeable && info.size > 0
	if !parallel {
		connections = 1
	} else {
		bytesPerConnection := max(opts.PartSize, int64(minBytesPerConnection))
		maxUseful := max(int((info.size+bytesPerConnection-1)/bytesPerConnection), 1)
		if connections > maxUseful {
			connections = maxUseful
		}
	}

	return Result{
		Output:      output,
		Size:        info.size,
		Rangeable:   info.rangeable,
		Discarded:   discard,
		FinalURL:    d.url,
		Connections: connections,
		Parallel:    parallel,
		PartSize:    opts.PartSize,
	}, nil
}

func (d *downloader) setCommonHeaders(req *http.Request) {
	req.Header.Set("User-Agent", d.ua)
	req.Header.Set("Accept-Encoding", "identity")
	for name, values := range d.headers {
		if strings.EqualFold(name, "Host") {
			if len(values) > 0 {
				req.Host = values[len(values)-1]
			}
			continue
		}
		req.Header.Del(name)
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
}
