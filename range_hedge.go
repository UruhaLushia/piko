package piko

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ErrSourceMismatch reports conflicting bytes from different sources.
var ErrSourceMismatch = errors.New("download sources returned different bytes")

const hedgeProbeBytes int64 = 64 * 1024

type hedgeResult struct {
	source int
	err    error
	data   []byte
}

func (d *downloader) hedgeRange(ctx context.Context, client *http.Client, p part) ([]byte, error) {
	first := p.source
	if d.hedgeNoGain.Load() >= 2 {
		return nil, nil
	}
	d.sourceMu.Lock()
	totalInflight := 0
	for i := range d.sourceState {
		totalInflight += d.sourceState[i].inflight
	}
	second := -1
	hedgeTimeout := 2 * time.Second
	for i := range d.sources {
		state := &d.sourceState[i]
		if totalInflight < d.sourceConcurrency && i != first && !state.disabled && !time.Now().Before(state.cooldown) && state.inflight < d.sourceLimit {
			second = i
			state.inflight++
			hedgeTimeout = max(hedgeTimeout, 4*state.ttfb, 4*d.sourceState[first].ttfb)
			break
		}
	}
	d.sourceMu.Unlock()
	if second < 0 {
		return nil, nil
	}
	defer d.releaseSource(second)
	end := min(p.end, p.start+hedgeProbeBytes-1)
	raceCtx, cancel := context.WithTimeout(ctx, hedgeTimeout)
	defer cancel()
	results := make(chan hedgeResult, 2)
	for _, source := range []int{first, second} {
		go func(source int) {
			started := time.Now()
			d.recordSourceAttempt(source)
			var data []byte
			var received int
			req, err := http.NewRequestWithContext(raceCtx, http.MethodGet, d.urlOf(source), nil)
			if err == nil {
				d.setCommonHeaders(req)
				d.setSourceHeaders(req, source)
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", d.rangeOffset+p.start, d.rangeOffset+end))
				resp, requestErr := client.Do(req)
				err = requestErr
				if err == nil {
					if err = validateRangeResponse(resp, p.index, d.rangeOffset+p.start, d.rangeOffset+end, d.remoteSize); err == nil {
						data = make([]byte, end-p.start+1)
						received, err = io.ReadFull(io.LimitReader(resp.Body, end-p.start+1), data)
						data = data[:received]
					}
					_ = resp.Body.Close()
				}
			}
			d.finishSource(source, int64(received), time.Since(started), err)
			results <- hedgeResult{source: source, err: err, data: data}
		}(source)
	}

	firstResult := <-results
	var secondResult hedgeResult
	if firstResult.err == nil {
		select {
		case secondResult = <-results:
		case <-time.After(25 * time.Millisecond):
			cancel()
			// Compare a fast loser, but do not hold up usable bytes for a stalled mirror.
			return firstResult.data, nil
		}
	} else {
		secondResult = <-results
	}
	overlap := min(len(firstResult.data), len(secondResult.data))
	if overlap > 0 && !bytes.Equal(firstResult.data[:overlap], secondResult.data[:overlap]) {
		d.hedgeMismatch.Store(true)
		return nil, ErrSourceMismatch
	}
	if firstResult.err == nil {
		if firstResult.source == first {
			d.hedgeNoGain.Add(1)
		} else {
			d.hedgeNoGain.Store(0)
		}
		return firstResult.data, nil
	}
	if secondResult.err == nil {
		return secondResult.data, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, nil
}
