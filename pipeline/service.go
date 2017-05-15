package pipeline

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	. "github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/base/request"
)

var builder errBuilder

type Pipeline struct {
	Config     *config.Config
	HTTPClient *http.Client
}

func NewConfig() *config.Config {
	return config.NewConfig()
}

func New(c *config.Config) (PipelineAPI, error) {
	return newClient(c)
}

func newClient(c *config.Config) (p *Pipeline, err error) {
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

	p = &Pipeline{
		Config:     c,
		HTTPClient: &http.Client{Transport: t},
	}

	return
}

func (c *Pipeline) newRequest(op *request.Operation, token string, v interface{}) *request.Request {
	req := request.New(c.Config, c.HTTPClient, op, token, builder, v)
	req.Data = v
	return req
}

func (c *Pipeline) newOperation(opName string, args ...interface{}) *request.Operation {
	var method, urlTmpl string
	switch opName {
	case OpCreateGroup:
		method, urlTmpl = MethodPost, "/v2/groups/%s"
	case OpUpdateGroup:
		method, urlTmpl = MethodPut, "/v2/groups/%s"
	case OpStartGroupTask:
		method, urlTmpl = MethodPost, "/v2/groups/%s/actions/start"
	case OpStopGroupTask:
		method, urlTmpl = MethodPost, "/v2/groups/%s/actions/stop"
	case OpListGroups:
		method, urlTmpl = MethodGet, "/v2/groups"
	case OpGetGroup:
		method, urlTmpl = MethodGet, "/v2/groups/%s"
	case OpDeleteGroup:
		method, urlTmpl = MethodDelete, "/v2/groups/%s"
	case OpCreateRepo:
		method, urlTmpl = MethodPost, "/v2/repos/%s"
	case OpUpdateRepo:
		method, urlTmpl = MethodPut, "/v2/repos/%s"
	case OpListRepos:
		method, urlTmpl = MethodGet, "/v2/repos"
	case OpGetRepo:
		method, urlTmpl = MethodGet, "/v2/repos/%s"
	case OpDeleteRepo:
		method, urlTmpl = MethodDelete, "/v2/repos/%s"
	case OpPostData:
		method, urlTmpl = MethodPost, "/v2/repos/%s/data"
	case OpCreateTransform:
		method, urlTmpl = MethodPost, "/v2/repos/%s/transforms/%s/to/%s"
	case OpUpdateTransform:
		method, urlTmpl = MethodPut, "/v2/repos/%s/transforms/%s"
	case OpListTransforms:
		method, urlTmpl = MethodGet, "/v2/repos/%s/transforms"
	case OpGetTransform:
		method, urlTmpl = MethodGet, "/v2/repos/%s/transforms/%s"
	case OpDeleteTransform:
		method, urlTmpl = MethodDelete, "/v2/repos/%s/transforms/%s"
	case OpCreateExport:
		method, urlTmpl = MethodPost, "/v2/repos/%s/exports/%s"
	case OpListExports:
		method, urlTmpl = MethodGet, "/v2/repos/%s/exports"
	case OpGetExport:
		method, urlTmpl = MethodGet, "/v2/repos/%s/exports/%s"
	case OpDeleteExport:
		method, urlTmpl = MethodDelete, "/v2/repos/%s/exports/%s"
	case OpUploadPlugin:
		method, urlTmpl = MethodPost, "/v2/plugins/%s"
	case OpListPlugins:
		method, urlTmpl = MethodGet, "/v2/plugins"
	case OpGetPlugin:
		method, urlTmpl = MethodGet, "/v2/plugins/%s"
	case OpDeletePlugin:
		method, urlTmpl = MethodDelete, "/v2/plugins/%s"
	case OpCreateDatasource:
		method, urlTmpl = MethodPost, "/v2/datasources/%s"
	case OpGetDatasource:
		method, urlTmpl = MethodGet, "/v2/datasources/%s"
	case OpListDatasources:
		method, urlTmpl = MethodGet, "/v2/datasources"
	case OpDeleteDatasource:
		method, urlTmpl = MethodDelete, "/v2/datasources/%s"
	case OpCreateJob:
		method, urlTmpl = MethodPost, "/v2/jobs/%s"
	case OpGetJob:
		method, urlTmpl = MethodGet, "/v2/jobs/%s"
	case OpListJobs:
		method, urlTmpl = MethodGet, "/v2/jobs%s"
	case OpDeleteJob:
		method, urlTmpl = MethodDelete, "/v2/jobs/%s"
	case OpStartJob:
		method, urlTmpl = MethodPost, "/v2/jobs/%s/actions/start"
	case OpStopJob:
		method, urlTmpl = MethodPost, "/v2/jobs/%s/actions/stop"
	case OpGetJobHistory:
		method, urlTmpl = MethodGet, "/v2/jobs/%s/history"
	case OpCreateJobExport:
		method, urlTmpl = MethodPost, "/v2/jobs/%s/exports/%s"
	case OpGetJobExport:
		method, urlTmpl = MethodGet, "/v2/jobs/%s/exports/%s"
	case OpListJobExports:
		method, urlTmpl = MethodGet, "/v2/jobs/%s/exports"
	case OpDeleteJobExport:
		method, urlTmpl = MethodDelete, "/v2/jobs/%s/exports/%s"
	case OpRetrieveSchema:
		method, urlTmpl = MethodPost, "/v2/schemas"
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
