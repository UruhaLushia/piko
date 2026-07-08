package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/UruhaLushia/piko"
)

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
	addressFamily, err := piko.ParseAddressFamily(opts.ipFamily)
	if err != nil {
		return err
	}
	headers := parseHeaders(opts.headers)
	var resolver piko.Resolver
	if len(opts.dnsServers) > 0 {
		resolver, err = piko.ParseResolver(opts.dnsServers...)
		if err != nil {
			return err
		}
	} else if opts.dns != "" {
		resolver, err = piko.ParseResolver(opts.dns)
		if err != nil {
			return err
		}
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
		AddressFamily:      addressFamily,
		Proxy:              opts.proxy,
		Resolver:           resolver,
		Started: func(result piko.Result) {
			if result.Parallel {
				fmt.Fprintf(os.Stdout, "parallel download: %s (%s, %d connections, adaptive pieces from %s)\n", result.Output, formatBytes(result.Size), result.Connections, formatBytes(result.PartSize))
				return
			}
			fmt.Fprintf(os.Stdout, "single connection: %s\n", result.Output)
		},
		Progress: printer.Update,
	})
	elapsed := time.Since(startedAt)
	if err != nil {
		printer.Abort()
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
