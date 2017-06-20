package logdb

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/base/request"
)

var builder errBuilder

type Logdb struct {
	Config     *config.Config
	HTTPClient *http.Client
}

func NewConfig() *config.Config {
	return config.NewConfig()
}

func New(c *config.Config) (LogdbAPI, error) {
	return newClient(c)
}

func newClient(c *config.Config) (p *Logdb, err error) {
	if !strings.HasPrefix(c.Endpoint, "http://") && !strings.HasPrefix(c.Endpoint, "https://") {
		err = fmt.Errorf("endpoint should start with 'http://' or 'https://'")
		return
	}
	if strings.HasSuffix(c.Endpoint, "/") {
		err = fmt.Errorf("endpoint should not end with '/'")
		return
	}

	var t = &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   c.DialTimeout,
			KeepAlive: 30 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: c.ResponseTimeout,
	}

	p = &Logdb{
		Config:     c,
		HTTPClient: &http.Client{Transport: t},
	}

	return
}

func (c *Logdb) newRequest(op *request.Operation, token string, v interface{}) *request.Request {
	req := request.New(c.Config, c.HTTPClient, op, token, builder, v)
	req.Data = v
	return req
}

func (c *Logdb) newOperation(opName string, args ...interface{}) *request.Operation {
	var method, urlTmpl string
	switch opName {
	case base.OpCreateRepo:
		method, urlTmpl = base.MethodPost, "/v5/repos/%s"
	case base.OpUpdateRepo:
		method, urlTmpl = base.MethodPut, "/v5/repos/%s"
	case base.OpListRepos:
		method, urlTmpl = base.MethodGet, "/v5/repos"
	case base.OpGetRepo:
		method, urlTmpl = base.MethodGet, "/v5/repos/%s"
	case base.OpDeleteRepo:
		method, urlTmpl = base.MethodDelete, "/v5/repos/%s"
	case base.OpSendLog:
		method, urlTmpl = base.MethodPost, "/v5/repos/%s/data?omitInvalidLog=%t"
	case base.OpQueryLog:
		method, urlTmpl = base.MethodGet, "/v5/repos/%s/search?q=%s&sort=%s&from=%d&size=%d&highlight=%t"
	case base.OpQueryHistogramLog:
		method, urlTmpl = base.MethodGet, "/v5/repos/%s/histogram?q=%s&from=%d&to=%d&field=%s"
	default:
		c.Config.Logger.Errorf("unmatched operation name: %s", opName)
		return nil
	}

	return &request.Operation{
		Name:   opName,
		Method: method,
		Path:   fmt.Sprintf(urlTmpl, args...),
	}
}
