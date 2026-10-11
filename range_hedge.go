package piko

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const hedgeProbeBytes int64 = 64 * 1024

type hedgeResult struct {
	source int
	err    error
}

func (d *downloader) hedgeRange(ctx context.Context, client *http.Client, p part) (int, bool) {
	first := p.source
	second := (first + 1) % len(d.sources)
	if d.sourceState[second].disabled {
		return first, false
	}
	end := min(p.end, p.start+hedgeProbeBytes-1)
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan hedgeResult, 2)
	for _, source := range []int{first, second} {
		go func(source int) {
			started := time.Now()
			d.recordSourceAttempt(source, 0)
			req, err := http.NewRequestWithContext(raceCtx, http.MethodGet, d.urlOf(source), nil)
			if err == nil {
				d.setCommonHeaders(req)
				d.setSourceHeaders(req, source)
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", d.rangeOffset+p.start, d.rangeOffset+end))
				var resp *http.Response
				resp, err = client.Do(req)
				if err == nil {
					if resp.StatusCode != http.StatusPartialContent {
						err = fmt.Errorf("hedge source returned %s", resp.Status)
					} else {
						var n int64
						n, err = io.Copy(io.Discard, io.LimitReader(resp.Body, end-p.start+1))
						if err == nil && n != end-p.start+1 {
							err = io.ErrUnexpectedEOF
						}
					}
					_ = resp.Body.Close()
				}
			}
			d.finishSource(source, end-p.start+1, time.Since(started), err)
			results <- hedgeResult{source: source, err: err}
		}(source)
	}
	for range 2 {
		result := <-results
		if result.err == nil {
			cancel()
			return result.source, true
		}
	}
	return first, false
}
