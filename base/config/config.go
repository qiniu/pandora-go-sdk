package config

import (
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
)

type Config struct {
	Endpoint        string
	Ak              string
	Sk              string
	Logger          base.Logger
	DialTimeout     time.Duration
	ResponseTimeout time.Duration
}

const (
	defaultDialTimeout     time.Duration = 10 * time.Second
	defaultResponseTimeout time.Duration = 30 * time.Second
)

func NewConfig() *Config {
	return &Config{
		DialTimeout:     defaultDialTimeout,
		ResponseTimeout: defaultResponseTimeout,
	}
}

func (c *Config) WithEndpoint(endpoint string) *Config {
	c.Endpoint = endpoint
	return c
}

func (c *Config) WithAccessKeySecretKey(ak, sk string) *Config {
	c.Ak, c.Sk = ak, sk
	return c
}

func (c *Config) WithDialTimeout(t time.Duration) *Config {
	c.DialTimeout = t
	return c
}

func (c *Config) WithResponseTimeout(t time.Duration) *Config {
	c.ResponseTimeout = t
	return c
}

func (c *Config) WithLogger(l base.Logger) *Config {
	c.Logger = l
	return c
}

func (c *Config) WithLoggerLevel(level base.LogLevelType) *Config {
	c.Logger.SetLoggerLevel(level)
	return c
}
