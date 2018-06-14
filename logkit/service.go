package logkit

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/base/request"
)

func NewConfig() *config.Config {
	return config.NewConfig()
}

type Logkit struct {
	config *config.Config
	client *http.Client
}

func New(c *config.Config) (*Logkit, error) {
	if len(c.LogkitEndpoint) == 0 {
		c.LogkitEndpoint = c.Endpoint
	}
	if len(c.LogkitEndpoint) == 0 {
		c.LogkitEndpoint = config.DefaultLogkitEndpoint
	}
	c.ConfigType = config.TypeLogkit
	if err := base.CheckEndPoint(c.LogkitEndpoint); err != nil {
		return nil, err
	}
	return &Logkit{
		config: c,
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   c.DialTimeout,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ResponseHeaderTimeout: c.ResponseTimeout,
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: c.AllowInsecureServer,
				},
			},
		},
	}, nil
}

func (l *Logkit) newRequest(op *request.Operation, token string, v interface{}) *request.Request {
	req := request.New(l.config, l.client, op, token, builder, v)
	req.Data = v
	return req
}
