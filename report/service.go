package report

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/base/request"
)

var builder errBuilder

type Report struct {
	Config     *config.Config
	HTTPClient *http.Client
}

func NewConfig() *config.Config {
	return config.NewConfig()
}

func New(c *config.Config) (ReportAPI, error) {
	return newClient(c)
}

func newClient(c *config.Config) (p *Report, err error) {
	if c.ReportEndpoint == "" {
		c.ReportEndpoint = c.Endpoint
	}
	if c.ReportEndpoint == "" {
		c.ReportEndpoint = config.DefaultReportEndpoint
	}
	c.ConfigType = config.TypeReport
	if err = base.CheckEndPoint(c.ReportEndpoint); err != nil {
		return
	}

	var t = &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   c.DialTimeout,
			KeepAlive: 30 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: c.ResponseTimeout,
	}

	p = &Report{
		Config:     c,
		HTTPClient: &http.Client{Transport: t},
	}

	return
}

func (c *Report) newRequest(op *request.Operation, token string, v interface{}) *request.Request {
	req := request.New(c.Config, c.HTTPClient, op, token, builder, v)
	req.Data = v
	return req
}

func (c *Report) newOperation(opName string, args ...interface{}) *request.Operation {
	var method, urlTmpl string
	switch opName {
	case base.OpActivateUser:
		method, urlTmpl = base.MethodPost, "/v1/activate"
	case base.OpCreateDatabase:
		method, urlTmpl = base.MethodGet, "/v1/dbs/%s"
	case base.OpListDatabases:
		method, urlTmpl = base.MethodGet, "/v1/dbs"
	case base.OpDeleteDatabase:
		method, urlTmpl = base.MethodDelete, "/v1/dbs/%s"
	case base.OpCreateTable:
		method, urlTmpl = base.MethodPost, "/v1/dbs/%s/tables/%s"
	case base.OpUpdateTable:
		method, urlTmpl = base.MethodDelete, "/v1/dbs/%s/tables/%s"
	case base.OpListTables:
		method, urlTmpl = base.MethodPost, "/v1/dbs/%s/tables"
	case base.OpDeleteTable:
		method, urlTmpl = base.MethodPost, "/v1/dbs/%s/tables/%s"
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
