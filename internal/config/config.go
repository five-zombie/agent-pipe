// Package config manages the per-project pipe.config.json file.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/five-zombie/agent-pipe/internal/id"
)

const (
	ConfigFileName   = "pipe.config.json"
	DefaultServerURL = "http://127.0.0.1:8080"
)

type Config struct {
	ClientID string       `json:"client_id"`
	Server   ServerConfig `json:"server"`
	Pipes    []Pipe       `json:"pipes"`
}

type ServerConfig struct {
	URL  string     `json:"url"`
	Auth AuthConfig `json:"auth"`
}

type AuthConfig struct {
	Token string `json:"token,omitempty"`
}

type Pipe struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Desc string `json:"decs"`
}

func DefaultPath() string {
	if path := os.Getenv("AIPIPE_CONFIG"); path != "" {
		return path
	}
	return ConfigFileName
}

func Default() (Config, error) {
	clientID, err := id.New()
	if err != nil {
		return Config{}, err
	}
	return Config{
		ClientID: clientID,
		Server:   ServerConfig{URL: DefaultServerURL},
		Pipes:    []Pipe{},
	}, nil
}

// Init creates a config only when path does not exist. Existing files are
// loaded and returned unchanged, preserving the skip-on-exists contract.
func Init(path string) (cfg Config, created bool, err error) {
	_, statErr := os.Stat(path)
	if statErr == nil {
		cfg, err = Load(path)
		return cfg, false, err
	}
	if !errors.Is(statErr, os.ErrNotExist) {
		return Config{}, false, fmt.Errorf("inspect config: %w", statErr)
	}

	cfg, err = Default()
	if err != nil {
		return Config{}, false, err
	}
	if err := Save(path, cfg); err != nil {
		return Config{}, false, err
	}
	return cfg, true, nil
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if cfg.ClientID == "" {
		return Config{}, errors.New("config client_id is required")
	}
	if cfg.Server.URL == "" {
		cfg.Server.URL = DefaultServerURL
	}
	if cfg.Pipes == nil {
		cfg.Pipes = []Pipe{}
	}
	return cfg, nil
}

func Save(path string, cfg Config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".pipe-config-*")
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("protect temporary config: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write config: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary config: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	return nil
}

func AddPipe(path string, cfg Config, pipe Pipe) (bool, error) {
	for _, existing := range cfg.Pipes {
		if existing.ID == pipe.ID {
			return false, nil
		}
	}
	cfg.Pipes = append(cfg.Pipes, pipe)
	return true, Save(path, cfg)
}

func ResolvePipe(cfg Config, target string) (Pipe, error) {
	for _, pipe := range cfg.Pipes {
		if pipe.ID == target {
			return pipe, nil
		}
	}
	for _, pipe := range cfg.Pipes {
		if pipe.Name == target {
			return pipe, nil
		}
	}
	return Pipe{}, fmt.Errorf("pipe %q is not registered", target)
}
