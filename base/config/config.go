package config

import (
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
)

type Config struct {
	Endpoint         string
	Ak               string
	Sk               string
	Logger           base.Logger
	DialTimeout      time.Duration
	ResponseTimeout  time.Duration
	RequestRateLimit int64 //每秒请求数限制
	FlowRateLimit    int64 //每秒流量限制(kb),若FlowRateLimit为100，则表示限速100KB/s
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

func (c *Config) WithRequestRateLimit(limit int64) *Config {
	c.RequestRateLimit = limit
	return c
}

func (c *Config) WithFlowRateLimit(limit int64) *Config {
	c.FlowRateLimit = limit
	return c
}
