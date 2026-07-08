# piko

A small parallel downloader for CLI and Go programs.

`piko` can save to a file, discard output for speed tests, or return downloaded bytes to your own code. It supports HTTP protocol selection, connection strategies, custom DNS resolvers, proxies, retries, and progress callbacks.

## Install

```bash
go install github.com/UruhaLushia/piko/cmd/piko@latest
```

Or download prebuilt binaries from GitHub Releases.

## CLI

```bash
piko [options] <url> [output]
```

Examples:

```bash
# Download with 32 range workers
piko -n 32 -o file.pkg https://example.com/file.pkg

# Speed test without saving
piko -n 32 -o NUL https://example.com/file.pkg
piko -n 32 -o /dev/null https://example.com/file.pkg

# Force HTTP/2 or HTTP/1.1
piko --http h2 https://example.com/file.pkg
piko --http h1.1 https://example.com/file.pkg

# Custom request headers
piko -H "Authorization: Bearer token" -H "Accept: application/octet-stream" https://example.com/file.pkg

# Spread parallel dials across resolved IPs (default)
piko -n 32 --connect-strategy round-robin https://example.com/file.pkg

# Race resolved IPs and use the fastest connection
piko -n 32 --connect-strategy fastest https://example.com/file.pkg

# Limit or prefer an IP family
piko -n 32 --ip-family ipv4 https://example.com/file.pkg
piko -n 32 --ip-family prefer-ipv4 https://example.com/file.pkg

# Proxy
piko --proxy http://127.0.0.1:7890 https://example.com/file.pkg
piko --proxy env https://example.com/file.pkg
piko --proxy direct https://example.com/file.pkg

# Custom DNS
piko --dns udp://1.1.1.1 https://example.com/file.pkg
piko --dns dot://1.1.1.1?name=cloudflare-dns.com https://example.com/file.pkg
piko --dns https://cloudflare-dns.com/dns-query https://example.com/file.pkg
```

Config is loaded from `~/.piko/config.yaml`, `~/.piko/config.yml`, `~/.piko/config.toml`, or `~/.piko/config.json`. CLI flags and positional output override config values.

```yaml
download:
  connections: 32
  part-size: 4MiB
http:
  protocol: h2
  user-agent: piko/1.0
  headers:
    - "Authorization: Bearer token"
network:
  proxy: direct
  dns: https://cloudflare-dns.com/dns-query
  ip-family: auto
```

Useful flags:

```text
    --config <path>             config file or directory (default ~/.piko)
-o, --output <path>             output file; discard with NUL on Windows or /dev/null on Unix
-f, --force                     overwrite output
-n, --connections <n>           parallel connections
    --retry <n>                 retry count
-k, --part-size <size>          max range part size, e.g. 4MiB
    --timeout <duration>        dial/header timeout
    --stall-timeout <duration>  cancel stalled reads
    --http <auto|h1|h2|h2c>     HTTP protocol
-H, --header <header>           custom request header, e.g. "Name: value"; repeatable
    --connect-strategy <mode>   IP strategy: round-robin (default), sequential, or fastest
    --ip-family <family>        auto, ipv4, ipv6, prefer-ipv4, or prefer-ipv6
    --proxy <url>               proxy URL, env, direct, or none (default direct)
    --dns <dns>                 system, udp://, tcp://, dot://, or https:// DoH URL
-A, --user-agent <ua>           user agent
```

## Library

Save to a file:

```go
package main

import (
	"context"
	"log"

	"github.com/UruhaLushia/piko"
)

func main() {
	result, err := piko.Download(context.Background(), "https://example.com/file.pkg", piko.Options{
		Output:      "file.pkg",
		Force:       true,
		Connections: 16,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("saved %s from %s", result.Output, result.FinalURL)
}
```

Return bytes for your own storage or transport:

```go
data, result, err := piko.DownloadBytes(ctx, "https://example.com/file.pkg", piko.Options{
	Connections: 8,
})
if err != nil {
	return err
}
_ = result
_ = data
```

Use custom DNS and proxy:

```go
resolver, err := dns.ParseResolver("https://cloudflare-dns.com/dns-query")
if err != nil {
	return err
}

client, err := piko.NewClient(piko.Options{
	Connections:        16,
	ConnectionStrategy: piko.ConnectionStrategyFastest,
	Headers: http.Header{
		"Authorization": {"Bearer token"},
	},
	Proxy:              "http://127.0.0.1:7890",
	Resolver:           resolver,
})
if err != nil {
	return err
}

result, err := client.Download(ctx, "https://example.com/file.pkg")
```

Import helper packages when needed:

```go
import (
	"net/http"

	"github.com/UruhaLushia/piko/dns"
)
```

## HTTP Client

You can let `piko` build an HTTP client:

```go
httpClient, err := piko.NewHTTPClient(piko.HTTPOptions{
	Protocol: piko.ProtocolHTTP2,
	Proxy:    "direct",
})
```

Or pass your own:

```go
client, err := piko.NewClient(piko.Options{
	HTTPClient: customHTTPClient,
})
```

For parallel range downloads, `HTTPClients` can be supplied when you want full control over per-worker clients.

## Releases

The GitHub Actions workflow builds CLI binaries for macOS, Linux, Android, and Windows. Pushes to `main` update `pre-release`; tags matching `v*` publish a normal release.

## License

GPL-3.0
