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
	client              *http.Client
	clients             []*http.Client
	selector            *dialer.Selector
	sourceURL           string
	url                 string
	sources             []Source
	sourceAttempts      []atomic.Int64
	sourceBytes         []atomic.Int64
	sourceStarted       []atomic.Int64
	nextSource          atomic.Uint32
	sourceMu            sync.Mutex
	sourceOutcome       []string
	ua                  string
	headers             http.Header
	retries             int
	stallTimeout        time.Duration
	progress            func(Progress)
	resume              bool
	concurrencyStrategy ConcurrencyStrategy
	resumed             int64
	total               int64
	remoteSize          int64
	rangeOffset         int64
	resumeETag          string
	resumeTime          string

	done atomic.Int64

	speedMu      sync.Mutex
	nextSpeedID  int64
	activeSpeeds map[int64]float64
}

func (d *downloader) chooseSource() int {
	if len(d.sources) < 2 {
		return 0
	}
	now := time.Now().UnixNano()
	best, bestRate := -1, float64(-1)
	for i := range d.sources {
		started := d.sourceStarted[i].Load()
		bytes := d.sourceBytes[i].Load()
		if started == 0 || bytes == 0 {
			continue
		}
		rate := float64(bytes) / float64(max(now-started, int64(time.Millisecond)))
		if rate > bestRate {
			best, bestRate = i, rate
		}
	}
	if best >= 0 {
		return best
	}
	return int(d.nextSource.Add(1)-1) % len(d.sources)
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
	for i := range opts.Sources {
		if opts.Sources[i].URL == "" {
			continue
		}
	}
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
	sources := normalizedSources(rawURL, opts.Sources)
	return &downloader{
		client:              client,
		clients:             clients,
		selector:            selector,
		sourceURL:           rawURL,
		ua:                  opts.UserAgent,
		headers:             opts.Headers,
		retries:             opts.Retries,
		stallTimeout:        opts.StallTimeout,
		progress:            opts.Progress,
		resume:              opts.Resume,
		concurrencyStrategy: opts.ConcurrencyStrategy,
		rangeOffset:         opts.Offset,
		sources:             sources,
		url:                 sources[0].URL,
		sourceAttempts:      make([]atomic.Int64, len(sources)),
		sourceBytes:         make([]atomic.Int64, len(sources)),
		sourceStarted:       make([]atomic.Int64, len(sources)),
		sourceOutcome:       make([]string, len(sources)),
	}
}

func normalizedSources(rawURL string, configured []Source) []Source {
	if len(configured) == 0 {
		return []Source{{URL: rawURL}}
	}
	out := make([]Source, 0, len(configured)+1)
	seen := make(map[string]struct{}, len(configured)+1)
	for _, source := range configured {
		if source.URL == "" {
			continue
		}
		if _, ok := seen[source.URL]; ok {
			continue
		}
		seen[source.URL] = struct{}{}
		out = append(out, source)
	}
	if len(out) == 0 {
		return []Source{{URL: rawURL}}
	}
	return out
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
		result.SourceStats = d.sourceStats()
		return result, err
	}
	result.SourceStats = d.sourceStats()
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
	d.filterSources(ctx, info.size)

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

func (d *downloader) filterSources(ctx context.Context, expectedSize int64) {
	if len(d.sources) < 2 || expectedSize <= 0 {
		return
	}
	valid := d.sources[:1]
	for i := 1; i < len(d.sources); i++ {
		info, err := d.probeRangeSource(ctx, d.sources[i].URL, i)
		if err != nil || (info.size > 0 && info.size != expectedSize) {
			d.setSourceOutcome(i, "rejected")
			continue
		}
		valid = append(valid, d.sources[i])
	}
	d.sources = valid
	d.sourceAttempts = make([]atomic.Int64, len(valid))
	d.sourceBytes = make([]atomic.Int64, len(valid))
	d.sourceStarted = make([]atomic.Int64, len(valid))
	d.sourceMu.Lock()
	d.sourceOutcome = make([]string, len(valid))
	d.sourceMu.Unlock()
}

func (d *downloader) recordSourceAttempt(source int, bytes int64) {
	if source < 0 || source >= len(d.sourceAttempts) {
		return
	}
	d.sourceAttempts[source].Add(1)
	d.sourceStarted[source].CompareAndSwap(0, time.Now().UnixNano())
	if bytes > 0 {
		d.sourceBytes[source].Add(bytes)
	}
	d.setSourceOutcome(source, "used")
}

func (d *downloader) recordSourceBytes(source int, bytes int64) {
	if source >= 0 && source < len(d.sourceBytes) && bytes > 0 {
		d.sourceBytes[source].Add(bytes)
	}
}

func (d *downloader) setSourceOutcome(source int, outcome string) {
	d.sourceMu.Lock()
	defer d.sourceMu.Unlock()
	if source >= 0 && source < len(d.sourceOutcome) {
		d.sourceOutcome[source] = outcome
	}
}

func (d *downloader) sourceStats() []SourceStat {
	stats := make([]SourceStat, len(d.sources))
	d.sourceMu.Lock()
	defer d.sourceMu.Unlock()
	for i, source := range d.sources {
		outcome := d.sourceOutcome[i]
		if outcome == "" {
			outcome = "unused"
		}
		stats[i] = SourceStat{URL: source.URL, Attempts: int(d.sourceAttempts[i].Load()), Bytes: d.sourceBytes[i].Load(), Outcome: outcome}
	}
	return stats
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

func (d *downloader) setSourceHeaders(req *http.Request, source int) {
	if source < 0 || source >= len(d.sources) {
		return
	}
	for name, values := range d.sources[source].Headers {
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
