package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type stringListConfig struct {
	Values []string
	Set    bool
}

func (l *stringListConfig) UnmarshalJSON(data []byte) error {
	return unmarshalConfigJSON(data, l.set)
}

func (l *stringListConfig) UnmarshalYAML(value *yaml.Node) error {
	return unmarshalConfigYAML(value, l.set)
}

func (l *stringListConfig) UnmarshalTOML(value any) error {
	return l.set(value)
}

func (l *stringListConfig) set(value any) error {
	values, err := stringListFromAny(value)
	if err != nil {
		return err
	}
	l.Values = values
	l.Set = true
	return nil
}

func stringListFromAny(value any) ([]string, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case string:
		return []string{v}, nil
	case []string:
		return v, nil
	case []any:
		values := make([]string, 0, len(v))
		for _, item := range v {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("list must contain only strings")
			}
			values = append(values, text)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("value must be a string or string array")
	}
}

func compactStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

type configDuration struct {
	Duration time.Duration
	Set      bool
}

func (d *configDuration) UnmarshalJSON(data []byte) error {
	return unmarshalConfigJSON(data, d.set)
}

func (d *configDuration) UnmarshalYAML(value *yaml.Node) error {
	return unmarshalConfigYAML(value, d.set)
}

func (d *configDuration) UnmarshalTOML(value any) error {
	return d.set(value)
}

func (d *configDuration) set(value any) error {
	duration, err := durationFromAny(value)
	if err != nil {
		return err
	}
	d.Duration = duration
	d.Set = true
	return nil
}

func durationFromAny(value any) (time.Duration, error) {
	switch v := value.(type) {
	case string:
		return time.ParseDuration(v)
	case int64:
		return time.Duration(v) * time.Second, nil
	case int:
		return time.Duration(v) * time.Second, nil
	case float64:
		return time.Duration(v * float64(time.Second)), nil
	default:
		return 0, fmt.Errorf("duration must be a string or seconds")
	}
}

func unmarshalConfigJSON(data []byte, set func(any) error) error {
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	return set(value)
}

func unmarshalConfigYAML(node *yaml.Node, set func(any) error) error {
	var value any
	if err := node.Decode(&value); err != nil {
		return err
	}
	return set(value)
}
