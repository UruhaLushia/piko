package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	Output            *string         `json:"output" yaml:"output" toml:"output"`
	Force             *bool           `json:"force" yaml:"force" toml:"force"`
	Connections       *int            `json:"connections" yaml:"connections" toml:"connections"`
	Retry             *int            `json:"retry" yaml:"retry" toml:"retry"`
	Retries           *int            `json:"retries" yaml:"retries" toml:"retries"`
	PartSize          *string         `json:"part-size" yaml:"part-size" toml:"part-size"`
	PartSizeSnake     *string         `json:"part_size" yaml:"part_size" toml:"part_size"`
	PartSizeCamel     *string         `json:"partSize" yaml:"partSize" toml:"partSize"`
	Timeout           *configDuration `json:"timeout" yaml:"timeout" toml:"timeout"`
	StallTimeout      *configDuration `json:"stall-timeout" yaml:"stall-timeout" toml:"stall-timeout"`
	StallTimeoutSnake *configDuration `json:"stall_timeout" yaml:"stall_timeout" toml:"stall_timeout"`
	StallTimeoutCamel *configDuration `json:"stallTimeout" yaml:"stallTimeout" toml:"stallTimeout"`
}

type httpConfig struct {
	Protocol       *string       `json:"protocol" yaml:"protocol" toml:"protocol"`
	UserAgent      *string       `json:"user-agent" yaml:"user-agent" toml:"user-agent"`
	UserAgentSnake *string       `json:"user_agent" yaml:"user_agent" toml:"user_agent"`
	UserAgentCamel *string       `json:"userAgent" yaml:"userAgent" toml:"userAgent"`
	Headers        configHeaders `json:"headers" yaml:"headers" toml:"headers"`
}

type networkConfig struct {
	Proxy                   *string         `json:"proxy" yaml:"proxy" toml:"proxy"`
	DNS                     *string         `json:"dns" yaml:"dns" toml:"dns"`
	ConnectStrategy         *string         `json:"connect-strategy" yaml:"connect-strategy" toml:"connect-strategy"`
	ConnectStrategySnake    *string         `json:"connect_strategy" yaml:"connect_strategy" toml:"connect_strategy"`
	ConnectStrategyCamel    *string         `json:"connectStrategy" yaml:"connectStrategy" toml:"connectStrategy"`
	ConnectionStrategy      *string         `json:"connection-strategy" yaml:"connection-strategy" toml:"connection-strategy"`
	ConnectionStrategySnake *string         `json:"connection_strategy" yaml:"connection_strategy" toml:"connection_strategy"`
	ConnectionStrategyCamel *string         `json:"connectionStrategy" yaml:"connectionStrategy" toml:"connectionStrategy"`
	IPFamily                *string         `json:"ip-family" yaml:"ip-family" toml:"ip-family"`
	IPFamilySnake           *string         `json:"ip_family" yaml:"ip_family" toml:"ip_family"`
	IPFamilyCamel           *string         `json:"ipFamily" yaml:"ipFamily" toml:"ipFamily"`
	AddressFamily           *string         `json:"address-family" yaml:"address-family" toml:"address-family"`
	AddressFamilySnake      *string         `json:"address_family" yaml:"address_family" toml:"address_family"`
	AddressFamilyCamel      *string         `json:"addressFamily" yaml:"addressFamily" toml:"addressFamily"`
	Timeout                 *configDuration `json:"timeout" yaml:"timeout" toml:"timeout"`
	StallTimeout            *configDuration `json:"stall-timeout" yaml:"stall-timeout" toml:"stall-timeout"`
	StallTimeoutSnake       *configDuration `json:"stall_timeout" yaml:"stall_timeout" toml:"stall_timeout"`
	StallTimeoutCamel       *configDuration `json:"stallTimeout" yaml:"stallTimeout" toml:"stallTimeout"`
}

type configHeaders []string

type configDuration time.Duration

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

	if value, ok := firstString(config.Download.Output); ok && !flagChanged(cmd, "output") {
		opts.output = value
	}
	if value, ok := firstBool(config.Download.Force); ok && !flagChanged(cmd, "force") {
		opts.force = value
	}
	if value, ok := firstInt(config.Download.Connections); ok && !flagChanged(cmd, "connections") {
		opts.connections = value
	}
	if value, ok := firstInt(config.Download.Retry, config.Download.Retries); ok && !flagChanged(cmd, "retry") {
		opts.retries = value
	}
	if value, ok := firstString(config.Download.PartSize, config.Download.PartSizeSnake, config.Download.PartSizeCamel); ok && !flagChanged(cmd, "part-size") {
		opts.partSize = value
	}
	if value, ok := firstDuration(config.Download.Timeout, config.Network.Timeout); ok && !flagChanged(cmd, "timeout") {
		opts.timeout = value
	}
	if value, ok := firstDuration(
		config.Download.StallTimeout,
		config.Download.StallTimeoutSnake,
		config.Download.StallTimeoutCamel,
		config.Network.StallTimeout,
		config.Network.StallTimeoutSnake,
		config.Network.StallTimeoutCamel,
	); ok && !flagChanged(cmd, "stall-timeout") {
		opts.stallTimeout = value
	}
	if value, ok := firstString(config.HTTP.Protocol); ok && !flagChanged(cmd, "http") {
		opts.protocol = value
	}
	if value, ok := firstString(
		config.Network.ConnectStrategy,
		config.Network.ConnectStrategySnake,
		config.Network.ConnectStrategyCamel,
		config.Network.ConnectionStrategy,
		config.Network.ConnectionStrategySnake,
		config.Network.ConnectionStrategyCamel,
	); ok && !flagChanged(cmd, "connect-strategy", "connection-strategy") {
		opts.strategy = value
	}
	if value, ok := firstString(
		config.Network.IPFamily,
		config.Network.IPFamilySnake,
		config.Network.IPFamilyCamel,
		config.Network.AddressFamily,
		config.Network.AddressFamilySnake,
		config.Network.AddressFamilyCamel,
	); ok && !flagChanged(cmd, "ip-family") {
		opts.ipFamily = value
	}
	if len(config.HTTP.Headers) > 0 && !flagChanged(cmd, "header") {
		opts.headers = []string(config.HTTP.Headers)
	}
	if value, ok := firstString(config.Network.Proxy); ok && !flagChanged(cmd, "proxy") {
		opts.proxy = value
	}
	if value, ok := firstString(config.Network.DNS); ok && !flagChanged(cmd, "dns") {
		opts.dns = value
	}
	if value, ok := firstString(config.HTTP.UserAgent, config.HTTP.UserAgentSnake, config.HTTP.UserAgentCamel); ok && !flagChanged(cmd, "user-agent") {
		opts.userAgent = value
	}
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
	for _, name := range names {
		if flags.Changed(name) {
			return true
		}
	}
	return false
}

func firstString(values ...*string) (string, bool) {
	for _, value := range values {
		if value != nil {
			return *value, true
		}
	}
	return "", false
}

func firstBool(values ...*bool) (bool, bool) {
	for _, value := range values {
		if value != nil {
			return *value, true
		}
	}
	return false, false
}

func firstInt(values ...*int) (int, bool) {
	for _, value := range values {
		if value != nil {
			return *value, true
		}
	}
	return 0, false
}

func firstDuration(values ...*configDuration) (time.Duration, bool) {
	for _, value := range values {
		if value != nil {
			return time.Duration(*value), true
		}
	}
	return 0, false
}

func (h *configHeaders) UnmarshalText(text []byte) error {
	*h = configHeaders{string(text)}
	return nil
}

func (h *configHeaders) UnmarshalJSON(data []byte) error {
	var values []string
	if err := json.Unmarshal(data, &values); err == nil {
		*h = values
		return nil
	}

	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("headers must be a string or string array")
	}
	*h = configHeaders{value}
	return nil
}

func (h *configHeaders) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.SequenceNode:
		headers := make([]string, 0, len(value.Content))
		for _, item := range value.Content {
			if item.Kind != yaml.ScalarNode {
				return fmt.Errorf("headers must contain only strings")
			}
			headers = append(headers, item.Value)
		}
		*h = headers
		return nil
	case yaml.ScalarNode:
		*h = configHeaders{value.Value}
		return nil
	default:
		return fmt.Errorf("headers must be a string or string array")
	}
}

func (d *configDuration) UnmarshalText(text []byte) error {
	duration, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = configDuration(duration)
	return nil
}

func (d *configDuration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err == nil {
		return d.UnmarshalText([]byte(value))
	}

	var seconds float64
	if err := json.Unmarshal(data, &seconds); err != nil {
		return fmt.Errorf("duration must be a string or seconds")
	}
	*d = configDuration(time.Duration(seconds * float64(time.Second)))
	return nil
}

func (d *configDuration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be a string or seconds")
	}
	if duration, err := time.ParseDuration(value.Value); err == nil {
		*d = configDuration(duration)
		return nil
	}

	var seconds float64
	if err := value.Decode(&seconds); err != nil {
		return fmt.Errorf("duration must be a string or seconds")
	}
	*d = configDuration(time.Duration(seconds * float64(time.Second)))
	return nil
}
