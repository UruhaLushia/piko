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
	sourceURL    string
	url          string
	ua           string
	headers      http.Header
	retries      int
	stallTimeout time.Duration
	progress     func(Progress)
	resume       bool
	resumed      int64
	total        int64
	remoteSize   int64
	rangeOffset  int64
	resumeETag   string
	resumeTime   string

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

// DownloadBytesRange downloads exactly length bytes starting at offset without
// probing the remote size first. A zero length returns an empty result.
func (c *Client) DownloadBytesRange(ctx context.Context, rawURL string, offset int64, length int64) ([]byte, Result, error) {
	opts := c.opts
	opts.Offset = offset
	opts.Length = length
	d := newDownloader(rawURL, opts, c.client, c.clients, c.selector)
	return d.runBytesRange(ctx, opts)
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

// DownloadBytesRange downloads a known byte range without probing the remote
// size first. Reuse Client.DownloadBytesRange for repeated range reads.
func DownloadBytesRange(ctx context.Context, rawURL string, offset int64, length int64, opts Options) ([]byte, Result, error) {
	client, err := NewClient(opts)
	if err != nil {
		return nil, Result{}, err
	}
	return client.DownloadBytesRange(ctx, rawURL, offset, length)
}

func newDownloader(rawURL string, opts Options, client *http.Client, clients []*http.Client, selector *dialer.Selector) *downloader {
	return &downloader{
		client:       client,
		clients:      clients,
		selector:     selector,
		sourceURL:    rawURL,
		url:          rawURL,
		ua:           opts.UserAgent,
		headers:      opts.Headers,
		retries:      opts.Retries,
		stallTimeout: opts.StallTimeout,
		progress:     opts.Progress,
		resume:       opts.Resume,
		rangeOffset:  opts.Offset,
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
	if result.Segmented {
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
	d.resumeETag = info.etag
	d.resumeTime = info.lastModified
	d.remoteSize = info.size
	if info.finalURL != "" {
		d.url = info.finalURL
		if finalURL, err := url.Parse(info.finalURL); err == nil && finalURL.Scheme != "" && finalURL.Host != "" {
			parsed = finalURL
		}
	}

	output := resolveOutputPath(opts.Output, parsed, info.suggested)
	discard := allowDiscard && IsNullOutput(output)

	selectionSize, partial, err := selectedRange(info.size, opts.Offset, opts.Length)
	if err != nil {
		return Result{}, err
	}
	if partial && (!info.rangeable || info.size <= 0) {
		return Result{}, fmt.Errorf("byte range requires a rangeable resource with a known size")
	}

	connections := opts.Connections
	if connections > 1 && info.rangeable && selectionSize > 0 {
		bytesPerConnection := max(opts.PartSize, int64(minBytesPerConnection))
		maxUseful := max(int((selectionSize+bytesPerConnection-1)/bytesPerConnection), 1)
		if connections > maxUseful {
			connections = maxUseful
		}
	}
	parallel := connections > 1 && info.rangeable && selectionSize > 0
	segmented := partial || parallel || (opts.Resume && info.rangeable && selectionSize > 0)
	if !segmented {
		connections = 1
	}

	return Result{
		Output:      output,
		Offset:      opts.Offset,
		Size:        selectionSize,
		TotalSize:   info.size,
		Rangeable:   info.rangeable,
		Discarded:   discard,
		FinalURL:    d.url,
		Connections: connections,
		Parallel:    parallel,
		PartSize:    opts.PartSize,
		Segmented:   segmented,
	}, nil
}

func selectedRange(total int64, offset int64, length int64) (int64, bool, error) {
	if offset < 0 {
		return 0, false, fmt.Errorf("negative offset %d", offset)
	}
	if length < 0 {
		return 0, false, fmt.Errorf("negative length %d", length)
	}
	partial := offset > 0 || length > 0
	if !partial || total <= 0 {
		return total, partial, nil
	}
	if offset >= total {
		return 0, true, fmt.Errorf("offset %d outside resource size %d", offset, total)
	}

	size := total - offset
	if length > 0 {
		size = min(size, length)
	}
	return size, true, nil
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
