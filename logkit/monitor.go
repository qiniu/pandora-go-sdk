package logkit

import (
	"github.com/google/go-querystring/query"

	. "github.com/qiniu/pandora-go-sdk/base/models"
)

type MetricsInfo struct {
	Opened   bool   `json:"opened"`
	RepoName string `json:"repo_name"`
	Interval int    `json:"interval"`
}

type GetMetricsInfoOptions struct {
	PandoraToken
}

// GetMetricsInfo 返回当前 metrics 的设定信息
func (l *Logkit) GetMetricsInfo(opts *GetMetricsInfoOptions) (*MetricsInfo, error) {
	op := newOperation(opGetMetricsInfo)
	resp := &MetricsInfo{}
	return resp, l.newRequest(op, opts.Token, resp).Send()
}

type AgentMetrics struct {
	Opened bool `json:"opened"`
	Data   map[string]struct {
		Buckets []map[string]interface{} `json:"buckets"`
	} `json:"data"`
}

type GetAgentMetricsOptions struct {
	PandoraToken `url:"-"`

	AgentID   string `url:"-"`
	BeginTime int64  `url:"beginTime,omitempty"`
	EndTime   int64  `url:"endTime,omitempty"`
}

// GetAgentMetrics 返回指定 agent 的具体 metrics 数据
func (l *Logkit) GetAgentMetrics(opts *GetAgentMetricsOptions) (*AgentMetrics, error) {
	vals, err := query.Values(opts)
	if err != nil {
		return nil, err
	}
	op := newOperation(opGetAgentMetrics, opts.AgentID, vals.Encode())
	resp := &AgentMetrics{}
	return resp, l.newRequest(op, opts.Token, resp).Send()
}
