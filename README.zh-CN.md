# piko

[English](README.md)

一个面向 CLI 和 Go 程序的小型分片下载器。

`piko` 可以保存到文件，也可以丢弃输出用于测速，或者把下载结果以字节返回给调用方自行处理。它支持 HTTP 协议选择、连接策略、自定义 DNS 解析器、代理、重试和进度回调。

`piko` 目前支持 HTTP Range 分片下载。普通单连接下载请直接使用 `curl`。

## 安装

```bash
go install github.com/UruhaLushia/piko/cmd/piko@latest
```

也可以从 GitHub Releases 下载预编译二进制文件。

## CLI

```bash
piko [flags] <url> [output]
```

示例：

```bash
# 使用 32 个 range worker 下载
piko -n 32 -o file.pkg https://example.com/file.pkg

# 继续中断的下载
piko -r -o file.pkg https://example.com/file.pkg

# 只测速，不保存文件
piko -n 32 -o NUL https://example.com/file.pkg
piko -n 32 -o /dev/null https://example.com/file.pkg

# 强制 HTTP/2 或 HTTP/1.1
piko --http h2 https://example.com/file.pkg
piko --http h1.1 https://example.com/file.pkg

# 自定义请求头
piko -H "Authorization: Bearer token" -H "Accept: application/octet-stream" https://example.com/file.pkg

# 在解析出的 IP 之间分散并发连接，默认策略
piko -n 32 --connect-strategy round-robin https://example.com/file.pkg

# 并发尝试解析出的 IP，使用最快连接
piko -n 32 --connect-strategy fastest https://example.com/file.pkg

# 限制或偏好 IP 类型
piko -n 32 --ip-family ipv4 https://example.com/file.pkg
piko -n 32 --ip-family prefer-ipv4 https://example.com/file.pkg

# 代理
piko --proxy http://127.0.0.1:7890 https://example.com/file.pkg
piko --proxy env https://example.com/file.pkg
piko --proxy direct https://example.com/file.pkg

# 自定义 DNS
piko --dns udp://1.1.1.1 https://example.com/file.pkg
piko --dns dot://cloudflare-dns.com https://example.com/file.pkg
piko --dns https://cloudflare-dns.com/dns-query https://example.com/file.pkg
```

配置会优先从 `~/.config/piko` 加载，未找到时再查找 `~/.piko`。两个目录均支持 `config.yaml`、`config.yml`、`config.toml` 和 `config.json`。CLI 参数和位置参数里的输出路径会覆盖配置值。

完整配置示例见 [examples/config.yaml](examples/config.yaml)。

常用参数：

```text
    --config <path>             配置文件或配置目录，默认 ~/.config/piko，其次 ~/.piko
-o, --output <path>             输出文件；Windows 可用 NUL 丢弃输出，Unix 可用 /dev/null
-f, --force                     覆盖输出文件
-r, --resume                    继续中断的 Range 下载
-n, --connections <n>           并发连接数
    --retry <n>                 重试次数
-s, --part-size <size>          初始分段大小，例如 4MiB
    --timeout <duration>        连接和响应头超时
    --stall-timeout <duration>  取消停滞的读取
    --http <auto|h1|h2|h2c>     HTTP 协议
-H, --header <header>           自定义请求头，例如 "Name: value"，可重复
    --connect-strategy <mode>   IP 策略：round-robin（默认）、sequential 或 fastest
    --ip-family <family>        auto、ipv4、ipv6、prefer-ipv4 或 prefer-ipv6
    --proxy <url>               代理 URL、env、direct 或 none，默认 direct
    --dns <dns>                 system、udp://、tcp://、dot:// 或 https:// DoH URL
-A, --user-agent <ua>           User-Agent
```

启用 `--resume` 后，piko 会使用 `file.pkg.part` 保存临时数据，并在 `file.pkg.part.resume` 中记录已完成的字节区间。再次使用相同 URL 和输出路径时，只下载缺失区间；复用前会检查远端大小以及 ETag 或 Last-Modified。

## 库

保存到文件：

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
		Resume:      true,
		Connections: 16,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("saved %s from %s", result.Output, result.FinalURL)
}
```

返回字节，由调用方自行保存或转发：

```go
data, result, err := piko.DownloadBytes(ctx, "https://example.com/file.pkg", piko.Options{
	Connections: 8,
	Offset:      1024,
	Length:      4096,
})
if err != nil {
	return err
}
_ = result
_ = data
```

`Offset` 从 0 开始，`Length: 0` 表示下载到远端文件末尾。范围下载中，`Result.Size` 是实际选区长度，`Result.TotalSize` 是远端完整大小。

重复随机读取远程文件时，可以复用客户端并跳过远端大小探测：

```go
client, err := piko.NewClient(piko.Options{Connections: 8})
if err != nil {
	return err
}
data, result, err := client.DownloadBytesRange(ctx, url, offset, length)
```

使用自定义 DNS 和代理：

```go
resolver, err := piko.ParseResolver("https://cloudflare-dns.com/dns-query")
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

## HTTP Client

可以让 `piko` 创建 HTTP client：

```go
httpClient, err := piko.NewHTTPClient(piko.HTTPOptions{
	Protocol: piko.ProtocolHTTP2,
	Proxy:    "direct",
})
```

也可以传入你自己的 client：

```go
client, err := piko.NewClient(piko.Options{
	HTTPClient: customHTTPClient,
})
```

如果想完全控制并行 range 下载的每个 worker，也可以传入 `HTTPClients`。

## 发布

GitHub Actions 会为 macOS、Linux、Android 和 Windows 构建 CLI 二进制文件。推送到 `main` 会更新 `pre-release`，匹配 `v*` 的 tag 会发布正式 release。

## 许可证

GPL-3.0
