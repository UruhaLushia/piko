package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const defaultConfigDirName = ".piko"

var configFileNames = []string{
	"config.yaml",
	"config.yml",
	"config.toml",
	"config.json",
}

type fileConfig struct {
	Download downloadConfig `json:"download" yaml:"download" toml:"download"`
	HTTP     httpConfig     `json:"http" yaml:"http" toml:"http"`
	Network  networkConfig  `json:"network" yaml:"network" toml:"network"`
}

type downloadConfig struct {
	Output       *string        `json:"output" yaml:"output" toml:"output"`
	Force        *bool          `json:"force" yaml:"force" toml:"force"`
	Connections  *int           `json:"connections" yaml:"connections" toml:"connections"`
	Retry        *int           `json:"retry" yaml:"retry" toml:"retry"`
	PartSize     *string        `json:"part-size" yaml:"part-size" toml:"part-size"`
	Timeout      configDuration `json:"timeout" yaml:"timeout" toml:"timeout"`
	StallTimeout configDuration `json:"stall-timeout" yaml:"stall-timeout" toml:"stall-timeout"`
}

type httpConfig struct {
	Protocol  *string          `json:"protocol" yaml:"protocol" toml:"protocol"`
	UserAgent *string          `json:"user-agent" yaml:"user-agent" toml:"user-agent"`
	Headers   stringListConfig `json:"headers" yaml:"headers" toml:"headers"`
}

type networkConfig struct {
	Proxy           *string          `json:"proxy" yaml:"proxy" toml:"proxy"`
	DNS             stringListConfig `json:"dns" yaml:"dns" toml:"dns"`
	ConnectStrategy *string          `json:"connect-strategy" yaml:"connect-strategy" toml:"connect-strategy"`
	IPFamily        *string          `json:"ip-family" yaml:"ip-family" toml:"ip-family"`
}

type stringListConfig struct {
	Values []string
	Set    bool
}

type configDuration struct {
	Duration time.Duration
	Set      bool
}

func defaultConfigDir() string {
	return "~/" + defaultConfigDirName
}

func applyConfig(cmd *cobra.Command, opts *cliOptions) error {
	config, err := readConfig(opts.config, cmd.Flags().Changed("config"))
	if err != nil {
		return err
	}
	if config == nil {
		return nil
	}

	applyValue(cmd, "output", &opts.output, config.Download.Output)
	applyValue(cmd, "force", &opts.force, config.Download.Force)
	applyValue(cmd, "connections", &opts.connections, config.Download.Connections)
	applyValue(cmd, "retry", &opts.retries, config.Download.Retry)
	applyValue(cmd, "part-size", &opts.partSize, config.Download.PartSize)
	if config.Download.Timeout.Set && !flagChanged(cmd, "timeout") {
		opts.timeout = config.Download.Timeout.Duration
	}
	if config.Download.StallTimeout.Set && !flagChanged(cmd, "stall-timeout") {
		opts.stallTimeout = config.Download.StallTimeout.Duration
	}
	applyValue(cmd, "http", &opts.protocol, config.HTTP.Protocol)
	applyValue(cmd, "connect-strategy", &opts.strategy, config.Network.ConnectStrategy)
	applyValue(cmd, "ip-family", &opts.ipFamily, config.Network.IPFamily)
	if config.HTTP.Headers.Set && !flagChanged(cmd, "header") {
		opts.headers = config.HTTP.Headers.Values
	}
	applyValue(cmd, "proxy", &opts.proxy, config.Network.Proxy)
	if config.Network.DNS.Set && !flagChanged(cmd, "dns") {
		opts.dnsServers = compactStrings(config.Network.DNS.Values)
	}
	applyValue(cmd, "user-agent", &opts.userAgent, config.HTTP.UserAgent)
	return nil
}

func readConfig(path string, required bool) (*fileConfig, error) {
	configPath, ok, err := resolveConfigFile(path, required)
	if err != nil || !ok {
		return nil, err
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) && !required {
			return nil, nil
		}
		return nil, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, nil
	}

	var config fileConfig
	switch strings.ToLower(filepath.Ext(configPath)) {
	case ".json":
		err = json.Unmarshal(data, &config)
	case ".yaml", ".yml":
		err = yaml.Unmarshal(data, &config)
	case ".toml":
		err = toml.Unmarshal(data, &config)
	default:
		err = fmt.Errorf("unsupported config format %q", filepath.Ext(configPath))
	}
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", configPath, err)
	}
	return &config, nil
}

func resolveConfigFile(path string, required bool) (string, bool, error) {
	if path == "" {
		return "", false, nil
	}

	path = expandHome(path)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) && !required {
			return "", false, nil
		}
		return "", false, err
	}
	if !info.IsDir() {
		return path, true, nil
	}

	for _, name := range configFileNames {
		candidate := filepath.Join(path, name)
		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() {
			return candidate, true, nil
		}
		if err != nil && !os.IsNotExist(err) {
			return "", false, err
		}
	}
	if required {
		return "", false, fmt.Errorf("no config file found in %s", path)
	}
	return "", false, nil
}

func expandHome(path string) string {
	if path == "~" || strings.HasPrefix(path, "~/") || strings.HasPrefix(path, "~\\") {
		home, err := os.UserHomeDir()
		if err == nil {
			return filepath.Join(home, strings.TrimLeft(path[1:], `/\`))
		}
	}
	return path
}

func flagChanged(cmd *cobra.Command, names ...string) bool {
	flags := cmd.Flags()
	return slices.ContainsFunc(names, flags.Changed)
}

func applyValue[T any](cmd *cobra.Command, flag string, target *T, value *T) {
	if value != nil && !flagChanged(cmd, flag) {
		*target = *value
	}
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
