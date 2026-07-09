package piko

import (
	"net/url"
	"strings"
	"sync"
)

var sharedFinalURLs finalURLStore

type finalURLStore struct {
	mu      sync.RWMutex
	entries map[string]string
}

func (s *finalURLStore) lookup(source string) (string, bool) {
	s.mu.RLock()
	target, ok := s.entries[source]
	s.mu.RUnlock()
	if !ok {
		return "", false
	}

	target, ok = normalizeFinalURL(source, target)
	if !ok {
		s.forget(source)
		return "", false
	}
	return target, true
}

func (s *finalURLStore) remember(source, target string) {
	target, ok := normalizeFinalURL(source, target)
	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		s.entries = make(map[string]string)
	}
	s.entries[source] = target
}

func (s *finalURLStore) forget(source string) {
	if source == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, source)
}

func normalizeFinalURL(source, target string) (string, bool) {
	target = strings.TrimSpace(target)
	if source == "" || target == "" || source == target {
		return "", false
	}
	parsed, err := url.Parse(target)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", false
	}
	return target, true
}
