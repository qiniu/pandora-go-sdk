package service

import (
	"fmt"
	"strings"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/logdb"
	"github.com/qiniu/pandora-go-sdk/pipeline"
	"github.com/qiniu/pandora-go-sdk/tsdb"
)

const (
	defaultTSDBEndpoint                   = "https://tsdb.qiniu.com"
	defaultLogDBEndpoint                  = "https://logdb.qiniu.com"
	defaultPipelineEndpoint               = "https://pipeline.qiniu.com"
	defaultDialTimeout      time.Duration = 10 * time.Second
	defaultResponseTimeout  time.Duration = 30 * time.Second
)

type Config struct {
	PipelineEndpoint string
	LogdbEndpoint    string
	TsdbEndpoint     string
	Ak               string
	Sk               string
	Logger           base.Logger
	DialTimeout      time.Duration
	ResponseTimeout  time.Duration
}

type Service struct {
	Pipeline pipeline.PipelineAPI
	LogDB    logdb.LogdbAPI
	TSDB     tsdb.TsdbAPI
}

func NewConfig() *Config {
	return &Config{
		DialTimeout:      defaultDialTimeout,
		ResponseTimeout:  defaultResponseTimeout,
		PipelineEndpoint: defaultPipelineEndpoint,
		LogdbEndpoint:    defaultLogDBEndpoint,
		TsdbEndpoint:     defaultTSDBEndpoint,
	}
}

func checkEndPoint(ep string) error {
	if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
		return fmt.Errorf("endpoint should start with 'http://' or 'https://'")
	}
	if strings.HasSuffix(ep, "/") {
		return fmt.Errorf("endpoint should not end with '/'")
	}
	return nil
}

func cloneConfig(conf *Config, endpoint string) *config.Config {
	return &config.Config{
		Endpoint:        endpoint,
		Ak:              conf.Ak,
		Sk:              conf.Sk,
		DialTimeout:     conf.DialTimeout,
		ResponseTimeout: conf.ResponseTimeout,
	}
}

func NewService(c *Config) (s *Service, err error) {

	if c.PipelineEndpoint != "" {
		if err = checkEndPoint(c.PipelineEndpoint); err != nil {
			return
		}
	} else {
		c.PipelineEndpoint = defaultPipelineEndpoint
	}
	if c.LogdbEndpoint != "" {
		if err = checkEndPoint(c.LogdbEndpoint); err != nil {
			return
		}
	} else {
		c.LogdbEndpoint = defaultLogDBEndpoint
	}
	if c.TsdbEndpoint != "" {
		if err = checkEndPoint(c.TsdbEndpoint); err != nil {
			return
		}
	} else {
		c.TsdbEndpoint = defaultTSDBEndpoint
	}

	pipelineAPI, err := pipeline.New(cloneConfig(c, c.PipelineEndpoint))
	if err != nil {
		return
	}
	logdbAPI, err := logdb.New(cloneConfig(c, c.LogdbEndpoint))
	if err != nil {
		return
	}
	tsdbAPI, err := tsdb.New(cloneConfig(c, c.TsdbEndpoint))
	if err != nil {
		return
	}
	s = &Service{
		Pipeline: pipelineAPI,
		LogDB:    logdbAPI,
		TSDB:     tsdbAPI,
	}
	return
}
