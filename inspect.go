package piko

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type remoteInfo struct {
	size      int64
	rangeable bool
	suggested string
	finalURL  string
}

func (d *downloader) inspect(ctx context.Context) (remoteInfo, error) {
	if info, ok, err := d.probeCachedFinalURL(ctx); ok || err != nil {
		return info, err
	}

	probed, probeErr := d.probeRange(ctx, d.url)
	if probeErr == nil && probed.downloadable() {
		d.rememberFinalURL(probed)
		return probed, nil
	}

	info, headStatus := d.inspectHead(ctx)
	if info.hasSizedRange() {
		d.rememberFinalURL(info)
		return info, nil
	}

	if probeErr != nil && (headStatus == 0 || headStatus >= 400) {
		return info, probeErr
	}
	return info, nil
}

func (d *downloader) probeCachedFinalURL(ctx context.Context) (remoteInfo, bool, error) {
	finalURL, ok := sharedFinalURLs.lookup(d.url)
	if !ok {
		return remoteInfo{}, false, nil
	}

	info, err := d.probeRange(ctx, finalURL)
	if err == nil && info.hasSizedRange() {
		d.rememberFinalURL(info)
		return info, true, nil
	}
	if err := ctx.Err(); err != nil {
		return remoteInfo{}, true, err
	}
	sharedFinalURLs.forget(d.url)
	return remoteInfo{}, false, nil
}

func (d *downloader) rememberFinalURL(info remoteInfo) {
	if info.hasSizedRange() {
		sharedFinalURLs.remember(d.url, info.finalURL)
	}
}

func (d *downloader) inspectHead(ctx context.Context) (remoteInfo, int) {
	info := remoteInfo{}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, d.url, nil)
	if err != nil {
		return info, 0
	}
	d.setCommonHeaders(req)
	resp, err := d.client.Do(req)
	if err != nil {
		return info, 0
	}
	resp.Body.Close()

	info.finalURL = resp.Request.URL.String()
	if resp.StatusCode < 400 {
		info.size = resp.ContentLength
		info.suggested = filenameFromDisposition(resp.Header.Get("Content-Disposition"))
		info.rangeable = strings.EqualFold(resp.Header.Get("Accept-Ranges"), "bytes")
	}
	return info, resp.StatusCode
}

func (i remoteInfo) downloadable() bool {
	return i.rangeable || i.size > 0
}

func (i remoteInfo) hasSizedRange() bool {
	return i.rangeable && i.size > 0
}

func (d *downloader) probeRange(ctx context.Context, rawURL string) (remoteInfo, error) {
	var lastErr error
	for attempt := 0; attempt <= d.retries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return remoteInfo{}, err
		}
		d.setCommonHeaders(req)
		req.Header.Set("Range", "bytes=0-0")

		resp, err := d.client.Do(req)
		if err != nil {
			lastErr = err
			if attempt < d.retries {
				if err := sleepWithContext(ctx, retryDelay(attempt)); err != nil {
					return remoteInfo{}, err
				}
			}
			continue
		}

		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1))
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusPartialContent {
			return remoteInfo{
				size:      parseContentRangeSize(resp.Header.Get("Content-Range")),
				rangeable: true,
				suggested: filenameFromDisposition(resp.Header.Get("Content-Disposition")),
				finalURL:  resp.Request.URL.String(),
			}, nil
		}
		if resp.StatusCode >= 400 {
			return remoteInfo{}, fmt.Errorf("range probe failed: %s", resp.Status)
		}
		return remoteInfo{
			size:      resp.ContentLength,
			rangeable: false,
			suggested: filenameFromDisposition(resp.Header.Get("Content-Disposition")),
			finalURL:  resp.Request.URL.String(),
		}, nil
	}
	return remoteInfo{}, lastErr
}
