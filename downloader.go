package piko

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

type downloader struct {
	client       *http.Client
	clients      []*http.Client
	url          string
	ua           string
	headers      http.Header
	retries      int
	stallTimeout time.Duration
	progress     func(Progress)
	total        int64

	done atomic.Int64
}

type Client struct {
	opts    Options
	client  *http.Client
	clients []*http.Client
}

func NewClient(opts Options) (*Client, error) {
	opts = opts.normalize()

	clients := compactHTTPClients(opts.HTTPClients)
	client := opts.HTTPClient
	switch {
	case client != nil:
		if len(clients) == 0 {
			clients = []*http.Client{client}
		}
	case len(clients) > 0:
		client = clients[0]
	default:
		var err error
		clients, err = newHTTPClients(opts.Connections, opts.Timeout, opts.Protocol, opts.ConnectionStrategy, opts.Proxy, opts.ProxyFunc, opts.Resolver)
		if err != nil {
			return nil, err
		}
		client = clients[0]
	}

	opts.Headers = cloneHeader(opts.Headers)
	return &Client{opts: opts, client: client, clients: clients}, nil
}

func (c *Client) Download(ctx context.Context, rawURL string) (Result, error) {
	d := newDownloader(rawURL, c.opts, c.client, c.clients)
	return d.run(ctx, c.opts)
}

// DownloadBytes downloads rawURL into memory without creating files.
func (c *Client) DownloadBytes(ctx context.Context, rawURL string) ([]byte, Result, error) {
	d := newDownloader(rawURL, c.opts, c.client, c.clients)
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

func newDownloader(rawURL string, opts Options, client *http.Client, clients []*http.Client) *downloader {
	return &downloader{
		client:       client,
		clients:      clients,
		url:          rawURL,
		ua:           opts.UserAgent,
		headers:      cloneHeader(opts.Headers),
		retries:      opts.Retries,
		stallTimeout: opts.StallTimeout,
		progress:     opts.Progress,
	}
}

func compactHTTPClients(clients []*http.Client) []*http.Client {
	compacted := clients[:0]
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
	plan, err := d.plan(ctx, opts, true)
	if err != nil {
		return Result{}, err
	}

	if !plan.result.Discarded {
		if err := prepareOutput(plan.result.Output, opts.Force); err != nil {
			return Result{}, err
		}
	}

	if opts.Started != nil {
		opts.Started(plan.result)
	}

	d.total = plan.info.size
	if plan.result.Parallel {
		err = d.downloadParts(ctx, plan.result.Output, plan.info.size, opts.PartSize, plan.result.Connections, opts.Force)
	} else {
		err = d.downloadSingle(ctx, plan.result.Output, plan.info.size, opts.Force)
	}
	if err != nil {
		return plan.result, err
	}
	return plan.result, nil
}

type downloadPlan struct {
	info   remoteInfo
	result Result
}

func (d *downloader) plan(ctx context.Context, opts Options, allowDiscard bool) (downloadPlan, error) {
	parsed, err := url.Parse(d.url)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return downloadPlan{}, fmt.Errorf("invalid url: %q", d.url)
	}

	info, err := d.inspect(ctx)
	if err != nil {
		return downloadPlan{}, err
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
		maxUseful := max(int((info.size+minDynamicPartSize-1)/minDynamicPartSize), 1)
		if connections > maxUseful {
			connections = maxUseful
		}
	}

	return downloadPlan{
		info: info,
		result: Result{
			Output:      output,
			Size:        info.size,
			Rangeable:   info.rangeable,
			Discarded:   discard,
			FinalURL:    d.url,
			Connections: connections,
			Parallel:    parallel,
			PartSize:    opts.PartSize,
		},
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
