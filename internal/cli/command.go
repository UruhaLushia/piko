package cli

import (
	"fmt"
	"time"

	"github.com/UruhaLushia/piko"
	"github.com/spf13/cobra"
)

type cliOptions struct {
	config       string
	output       string
	connections  int
	retries      int
	force        bool
	partSize     string
	timeout      time.Duration
	stallTimeout time.Duration
	protocol     string
	strategy     string
	ipFamily     string
	headers      []string
	proxy        string
	dns          string
	dnsServers   []string
	userAgent    string
}

func NewRootCommand() *cobra.Command {
	defaults := piko.DefaultOptions()
	opts := cliOptions{
		config:       defaultConfigDir(),
		connections:  defaults.Connections,
		retries:      defaults.Retries,
		partSize:     formatBytes(defaults.PartSize),
		timeout:      defaults.Timeout,
		stallTimeout: defaults.StallTimeout,
		protocol:     defaults.Protocol.String(),
		strategy:     defaults.ConnectionStrategy.String(),
		ipFamily:     defaults.AddressFamily.String(),
		userAgent:    defaults.UserAgent,
	}

	cmd := &cobra.Command{
		Use:           "piko <url> [output]",
		Short:         "A small parallel downloader",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 2 {
				return fmt.Errorf("too many positional arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return cmd.Help()
			}
			if err := applyConfig(cmd, &opts); err != nil {
				return err
			}
			if len(args) > 1 {
				opts.output = args[1]
			}
			return run(cmd.Context(), args[0], opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.config, "config", opts.config, "config file or directory")
	flags.StringVarP(&opts.output, "output", "o", "", "output file; discard with NUL on Windows or /dev/null on Unix")
	flags.BoolVarP(&opts.force, "force", "f", false, "overwrite output")
	flags.IntVarP(&opts.connections, "connections", "n", opts.connections, "parallel connections")
	flags.IntVar(&opts.retries, "retry", opts.retries, "retry count")
	flags.StringVarP(&opts.partSize, "part-size", "s", opts.partSize, "initial range part size")
	flags.DurationVar(&opts.timeout, "timeout", opts.timeout, "dial/header timeout")
	flags.DurationVar(&opts.stallTimeout, "stall-timeout", opts.stallTimeout, "cancel stalled reads")
	flags.StringVar(&opts.protocol, "http", opts.protocol, "HTTP protocol: auto, h1, h2, h2c")
	flags.StringVar(&opts.strategy, "connect-strategy", opts.strategy, "IP connection strategy: round-robin, sequential, fastest")
	flags.StringVar(&opts.ipFamily, "ip-family", opts.ipFamily, "IP family: auto, ipv4, ipv6, prefer-ipv4, prefer-ipv6")
	flags.StringArrayVarP(&opts.headers, "header", "H", nil, `custom request header, e.g. "Name: value"; repeatable`)
	flags.StringVar(&opts.proxy, "proxy", "", "proxy URL, env, direct, or none (default direct)")
	flags.StringVar(&opts.dns, "dns", "", "DNS: system, udp://1.1.1.1, dot://1.1.1.1, or https://.../dns-query")
	flags.StringVarP(&opts.userAgent, "user-agent", "A", opts.userAgent, "user agent")
	return cmd
}
