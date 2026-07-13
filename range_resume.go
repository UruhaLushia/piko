package piko

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	resumeStateVersion       = 1
	resumeCheckpointInterval = 2 * time.Second
)

type downloadSpan struct {
	start int64
	end   int64
}

func (r downloadSpan) length() int64 {
	return r.end - r.start + 1
}

type resumeIdentity struct {
	sourceKey    string
	offset       int64
	size         int64
	totalSize    int64
	etag         string
	lastModified string
}

type resumeState struct {
	Version      int           `json:"version"`
	SourceKey    string        `json:"source_key"`
	Offset       int64         `json:"offset"`
	Size         int64         `json:"size"`
	TotalSize    int64         `json:"total_size"`
	ETag         string        `json:"etag,omitempty"`
	LastModified string        `json:"last_modified,omitempty"`
	Completed    []resumeRange `json:"completed,omitempty"`
}

type resumeRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type resumeTracker struct {
	file           *os.File
	path           string
	identity       resumeIdentity
	completed      []downloadSpan
	lastCheckpoint time.Time
}

func (d *downloader) resumeIdentity(size int64) resumeIdentity {
	return resumeIdentity{
		sourceKey:    d.resumeSourceKey(),
		offset:       d.rangeOffset,
		size:         size,
		totalSize:    d.remoteSize,
		etag:         d.resumeETag,
		lastModified: d.resumeTime,
	}
}

func (d *downloader) resumeSourceKey() string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(d.sourceURL))
	names := make([]string, 0, len(d.headers))
	for name := range d.headers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(name))
		for _, value := range d.headers[name] {
			_, _ = hash.Write([]byte{0})
			_, _ = hash.Write([]byte(value))
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func (d *downloader) resumeValidator() string {
	if !d.resume {
		return ""
	}
	if d.resumeETag != "" && !strings.HasPrefix(strings.TrimSpace(d.resumeETag), "W/") {
		return d.resumeETag
	}
	return d.resumeTime
}

func openResumeFile(partPath string, identity resumeIdentity) (*os.File, []downloadSpan, *resumeTracker, error) {
	statePath := resumeStatePath(partPath)
	state, stateErr := readResumeState(statePath)
	if stateErr == nil && state.matches(identity) {
		completed, ok := state.completedSpans(identity.size)
		if ok {
			file, err := os.OpenFile(partPath, os.O_RDWR, 0o644)
			if err == nil {
				info, statErr := file.Stat()
				if statErr == nil && info.Mode().IsRegular() && info.Size() == identity.size {
					tracker := newResumeTracker(file, statePath, identity, completed)
					return file, completed, tracker, nil
				}
				_ = file.Close()
			}
		}
	}

	if err := discardResumeFiles(partPath, statePath); err != nil {
		return nil, nil, nil, err
	}
	file, err := os.OpenFile(partPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := file.Truncate(identity.size); err != nil {
		_ = file.Close()
		_ = discardResumeFiles(partPath, statePath)
		return nil, nil, nil, err
	}
	tracker := newResumeTracker(file, statePath, identity, nil)
	if err := tracker.Checkpoint(); err != nil {
		_ = file.Close()
		_ = discardResumeFiles(partPath, statePath)
		return nil, nil, nil, err
	}
	return file, nil, tracker, nil
}

func newResumeTracker(file *os.File, path string, identity resumeIdentity, completed []downloadSpan) *resumeTracker {
	return &resumeTracker{
		file:           file,
		path:           path,
		identity:       identity,
		completed:      append([]downloadSpan(nil), completed...),
		lastCheckpoint: time.Now(),
	}
}

func (t *resumeTracker) Record(offset int64, size int) error {
	if size <= 0 {
		return nil
	}
	t.completed = append(t.completed, downloadSpan{start: offset, end: offset + int64(size) - 1})
	t.completed = normalizeDownloadSpans(t.completed)
	if time.Since(t.lastCheckpoint) < resumeCheckpointInterval {
		return nil
	}
	return t.Checkpoint()
}

func (t *resumeTracker) Checkpoint() error {
	if err := t.file.Sync(); err != nil {
		return err
	}
	state := resumeState{
		Version:      resumeStateVersion,
		SourceKey:    t.identity.sourceKey,
		Offset:       t.identity.offset,
		Size:         t.identity.size,
		TotalSize:    t.identity.totalSize,
		ETag:         t.identity.etag,
		LastModified: t.identity.lastModified,
		Completed:    make([]resumeRange, len(t.completed)),
	}
	for i, completed := range t.completed {
		state.Completed[i] = resumeRange{Start: completed.start, End: completed.end}
	}
	if err := writeResumeState(t.path, state); err != nil {
		return err
	}
	t.lastCheckpoint = time.Now()
	return nil
}

func (s resumeState) matches(identity resumeIdentity) bool {
	if s.Version != resumeStateVersion || s.SourceKey != identity.sourceKey || s.Offset != identity.offset || s.Size != identity.size || s.TotalSize != identity.totalSize {
		return false
	}
	if s.ETag != "" || identity.etag != "" {
		return s.ETag == identity.etag
	}
	if s.LastModified != "" || identity.lastModified != "" {
		return s.LastModified == identity.lastModified
	}
	return true
}

func (s resumeState) completedSpans(size int64) ([]downloadSpan, bool) {
	spans := make([]downloadSpan, len(s.Completed))
	for i, completed := range s.Completed {
		if completed.Start < 0 || completed.End < completed.Start || completed.End >= size {
			return nil, false
		}
		spans[i] = downloadSpan{start: completed.Start, end: completed.End}
	}
	return normalizeDownloadSpans(spans), true
}

func normalizeDownloadSpans(spans []downloadSpan) []downloadSpan {
	if len(spans) < 2 {
		return spans
	}
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].start < spans[j].start
	})
	merged := spans[:0]
	for _, span := range spans {
		last := len(merged) - 1
		if last >= 0 && span.start <= merged[last].end+1 {
			merged[last].end = max(merged[last].end, span.end)
			continue
		}
		merged = append(merged, span)
	}
	return merged
}

func missingDownloadSpans(size int64, completed []downloadSpan) []downloadSpan {
	missing := make([]downloadSpan, 0, len(completed)+1)
	cursor := int64(0)
	for _, span := range completed {
		if cursor < span.start {
			missing = append(missing, downloadSpan{start: cursor, end: span.start - 1})
		}
		cursor = span.end + 1
	}
	if cursor < size {
		missing = append(missing, downloadSpan{start: cursor, end: size - 1})
	}
	return missing
}

func downloadSpanBytes(spans []downloadSpan) int64 {
	var total int64
	for _, span := range spans {
		total += span.length()
	}
	return total
}

func resumeStatePath(partPath string) string {
	return partPath + ".resume"
}

func readResumeState(path string) (resumeState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return resumeState{}, err
	}
	var state resumeState
	if err := json.Unmarshal(data, &state); err != nil {
		return resumeState{}, err
	}
	return state, nil
}

func writeResumeState(path string, state resumeState) error {
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)

	encoder := json.NewEncoder(temp)
	if err := encoder.Encode(state); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}

func discardResumeFiles(partPath, statePath string) error {
	for _, path := range []string{partPath, statePath} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
