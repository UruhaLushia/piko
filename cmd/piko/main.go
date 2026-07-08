package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UruhaLushia/piko"
	"github.com/UruhaLushia/piko/dns"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

const (
	progressInterval = 500 * time.Millisecond
	progressBarWidth = 28
)

type cliOptions struct {
	output       string
	connections  int
	retries      int
	force        bool
	partSize     string
	timeout      time.Duration
	stallTimeout time.Duration
	protocol     string
	strategy     string
	headers      []string
	proxy        string
	noProxy      bool
	dns          string
	userAgent    string
}

func main() {
	cmd := newRootCommand()
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	defaults := piko.DefaultOptions()
	opts := cliOptions{
		connections:  defaults.Connections,
		retries:      defaults.Retries,
		partSize:     formatBytes(defaults.PartSize),
		timeout:      defaults.Timeout,
		stallTimeout: defaults.StallTimeout,
		protocol:     defaults.Protocol.String(),
		strategy:     defaults.ConnectionStrategy.String(),
		userAgent:    defaults.UserAgent,
	}

	cmd := &cobra.Command{
		Use:           "piko [options] <url> [output]",
		Short:         "A small parallel downloader",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("missing url")
			}
			if len(args) > 2 {
				return fmt.Errorf("too many positional arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				opts.output = args[1]
			}
			return run(cmd.Context(), args[0], opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&opts.output, "output", "o", "", "output path or NUL / /dev/null")
	flags.BoolVarP(&opts.force, "force", "f", false, "overwrite output")
	flags.IntVarP(&opts.connections, "connections", "n", opts.connections, "parallel connections")
	flags.IntVar(&opts.retries, "retry", opts.retries, "retry count")
	flags.StringVarP(&opts.partSize, "part-size", "k", opts.partSize, "range part size")
	flags.DurationVar(&opts.timeout, "timeout", opts.timeout, "dial/header timeout")
	flags.DurationVar(&opts.stallTimeout, "stall-timeout", opts.stallTimeout, "cancel stalled reads")
	flags.StringVar(&opts.protocol, "http", opts.protocol, "HTTP protocol: auto, h1, h2, h2c")
	flags.StringVar(&opts.strategy, "connect-strategy", opts.strategy, "IP connection strategy: sequential, round-robin, fastest")
	flags.StringVar(&opts.strategy, "connection-strategy", opts.strategy, "IP connection strategy alias for --connect-strategy")
	flags.StringArrayVarP(&opts.headers, "header", "H", nil, "custom request header, repeatable")
	flags.StringVar(&opts.proxy, "proxy", "", "proxy URL, env, direct, or none")
	flags.BoolVar(&opts.noProxy, "no-proxy", false, "disable proxy")
	flags.StringVar(&opts.dns, "dns", "", "resolver: system, udp://1.1.1.1, dot://1.1.1.1, or https://.../dns-query")
	flags.StringVar(&opts.dns, "resolver", "", "resolver alias for --dns")
	flags.StringVar(&opts.userAgent, "ua", opts.userAgent, "user agent")
	flags.StringVar(&opts.userAgent, "user-agent", opts.userAgent, "user agent")
	return cmd
}

func run(ctx context.Context, rawURL string, opts cliOptions) error {
	partSize, err := parseSize(opts.partSize)
	if err != nil {
		return fmt.Errorf("--part-size: %w", err)
	}
	protocol, err := piko.ParseProtocol(opts.protocol)
	if err != nil {
		return err
	}
	strategy, err := piko.ParseConnectionStrategy(opts.strategy)
	if err != nil {
		return err
	}
	headers := parseHeaders(opts.headers)
	var resolver piko.Resolver
	if opts.dns != "" {
		resolver, err = dns.ParseResolver(opts.dns)
		if err != nil {
			return err
		}
	}
	proxy := opts.proxy
	if opts.noProxy {
		proxy = "direct"
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	printer := newProgressPrinter(os.Stdout)
	startedAt := time.Now()
	result, err := piko.Download(ctx, rawURL, piko.Options{
		Output:             opts.output,
		Connections:        opts.connections,
		Retries:            opts.retries,
		Force:              opts.force,
		PartSize:           partSize,
		Timeout:            opts.timeout,
		StallTimeout:       opts.stallTimeout,
		UserAgent:          opts.userAgent,
		Headers:            headers,
		Protocol:           protocol,
		ConnectionStrategy: strategy,
		Proxy:              proxy,
		Resolver:           resolver,
		Started: func(result piko.Result) {
			if result.Parallel {
				fmt.Fprintf(os.Stdout, "parallel download: %s (%s, %d connections, pieces %s)\n", result.Output, formatBytes(result.Size), result.Connections, formatBytes(result.PartSize))
				return
			}
			fmt.Fprintf(os.Stdout, "single connection: %s\n", result.Output)
		},
		Progress: printer.Update,
	})
	elapsed := time.Since(startedAt)
	if err != nil {
		printer.Done()
		return fmt.Errorf("failed after %s: %w", formatDuration(elapsed), err)
	}

	printer.Done()
	size := result.Size
	if size <= 0 {
		size = printer.Bytes()
	}
	fmt.Printf("finished: %s in %s, avg %s/s\n", formatBytes(size), formatDuration(elapsed), formatBytes(averageSpeed(size, elapsed)))
	if result.Discarded {
		fmt.Println("discarded:", result.Output)
	} else {
		fmt.Println("saved:", result.Output)
	}
	return nil
}

func parseHeaders(values []string) http.Header {
	if len(values) == 0 {
		return nil
	}

	headers := make(http.Header)
	for _, value := range values {
		name, headerValue, _ := strings.Cut(value, ":")
		headers.Add(name, strings.TrimSpace(headerValue))
	}
	return headers
}

func parseSize(value string) (int64, error) {
	text := strings.TrimSpace(strings.ToLower(value))
	if text == "" {
		return 0, fmt.Errorf("empty size")
	}

	multiplier := int64(1)
	for _, suffix := range []struct {
		text string
		mul  int64
	}{
		{"kib", 1024},
		{"kb", 1024},
		{"k", 1024},
		{"mib", 1024 * 1024},
		{"mb", 1024 * 1024},
		{"m", 1024 * 1024},
		{"gib", 1024 * 1024 * 1024},
		{"gb", 1024 * 1024 * 1024},
		{"g", 1024 * 1024 * 1024},
	} {
		if strings.HasSuffix(text, suffix.text) {
			multiplier = suffix.mul
			text = strings.TrimSpace(strings.TrimSuffix(text, suffix.text))
			break
		}
	}
	if text == "" {
		return 0, fmt.Errorf("missing number")
	}
	valueFloat, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, err
	}
	if valueFloat <= 0 {
		return 0, fmt.Errorf("must be positive")
	}
	return int64(valueFloat * float64(multiplier)), nil
}

type progressPrinter struct {
	w        io.Writer
	mu       sync.Mutex
	bar      *progressbar.ProgressBar
	total    int64
	finished bool
	latest   piko.Progress
}

func newProgressPrinter(w io.Writer) *progressPrinter {
	return &progressPrinter{w: w}
}

func (p *progressPrinter) Update(progress piko.Progress) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.latest = progress
	p.ensureBarLocked(progress.Total)
	current := progress.Bytes
	if progress.Total > 0 && current > progress.Total {
		current = progress.Total
	}
	_ = p.bar.Set64(current)
	if progress.Done {
		p.finished = true
		_ = p.bar.Finish()
	}
}

func (p *progressPrinter) Done() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	p.finished = true
	p.ensureBarLocked(p.latest.Total)
	_ = p.bar.Finish()
}

func (p *progressPrinter) Bytes() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.latest.Bytes
}

func (p *progressPrinter) ensureBarLocked(total int64) {
	maxBytes := total
	if maxBytes <= 0 {
		maxBytes = -1
	}
	if p.bar == nil {
		p.total = maxBytes
		p.bar = progressbar.NewOptions64(
			maxBytes,
			progressbar.OptionSetWriter(p.w),
			progressbar.OptionSetWidth(progressBarWidth),
			progressbar.OptionSetTheme(progressbar.ThemeASCII),
			progressbar.OptionSetRenderBlankState(true),
			progressbar.OptionUseANSICodes(true),
			progressbar.OptionShowBytes(true),
			progressbar.OptionShowTotalBytes(true),
			progressbar.OptionShowCount(),
			progressbar.OptionUseIECUnits(true),
			progressbar.OptionThrottle(progressInterval),
			progressbar.OptionOnCompletion(func() {
				fmt.Fprintln(p.w)
			}),
			progressbar.OptionSpinnerType(14),
		)
		return
	}

	if maxBytes > 0 && p.total != maxBytes {
		p.total = maxBytes
		p.bar.ChangeMax64(maxBytes)
	}
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for value := n / unit; value >= unit; value /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func averageSpeed(bytes int64, elapsed time.Duration) int64 {
	if bytes <= 0 || elapsed <= 0 {
		return 0
	}
	return int64(float64(bytes) / elapsed.Seconds())
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	if d < time.Minute {
		return d.Round(10 * time.Millisecond).String()
	}
	return d.Round(time.Second).String()
}
