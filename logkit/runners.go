package logkit

import (
	"encoding/json"
	"errors"

	"github.com/google/go-querystring/query"

	"github.com/qiniu/pandora-go-sdk/base"
	. "github.com/qiniu/pandora-go-sdk/base/models"
)

type Runner struct {
	UID             string      `json:"uid"`
	Name            string      `json:"name"`
	State           string      `json:"state"`
	Type            string      `json:"type"`
	AgentID         string      `json:"agent_id"`
	Config          interface{} `json:"config"`
	Status          interface{} `json:"status"`
	LastUpdate      int64       `json:"update_time"`
	ConfigTimestamp int64       `json:"config_timestamp"`
	Error           interface{} `json:"error"`
}

type GetRunnersOptions struct {
	PandoraToken `url:"-"`

	Type          string `url:"type"`
	Tag           string `url:"tag"`
	ID            string `url:"id"`
	Name          string `url:"name"`
	IncludeAgents bool   `url:"agents"`
	Sort          string `url:"sort"`
	Order         string `url:"order"`
	Search        string `url:"search"`

	Page int `url:"page"`
	Size int `url:"size"`
}

// GetRunners 返回符合条件的 runners、agents（可选）信息以及可获取的总数
func (l *Logkit) GetRunners(opts *GetRunnersOptions) ([]*Runner, map[string]interface{}, int, error) {
	vals, err := query.Values(opts)
	if err != nil {
		return nil, nil, 0, err
	}
	op := newOperation(opGetRunners, vals.Encode())
	resp := &struct {
		Runners   []*Runner              `json:"runners"`
		Agents    map[string]interface{} `json:"agents"`
		TotalSize int                    `json:"totalSize"`
	}{}
	return resp.Runners, resp.Agents, resp.TotalSize, l.newRequest(op, opts.Token, resp).Send()
}

type RunnerCond struct {
	ConfigName string `json:"config_name"`
	AgentID    string `json:"agent_id"`
}

type BatchRunnersOptions struct {
	PandoraToken

	RunnerConds []RunnerCond
}

// StartRunners 启动指定 runners
func (l *Logkit) StartRunners(opts *BatchRunnersOptions) error {
	op := newOperation(opStartRunners)
	data, err := json.Marshal(opts.RunnerConds)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

// StopRunners 停止指定 runners
func (l *Logkit) StopRunners(opts *BatchRunnersOptions) error {
	op := newOperation(opStopRunners)
	data, err := json.Marshal(opts.RunnerConds)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

// ResetRunners 重置指定 runners
func (l *Logkit) ResetRunners(opts *BatchRunnersOptions) error {
	op := newOperation(opResetRunners)
	data, err := json.Marshal(opts.RunnerConds)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

// DeleteRunners 删除指定 runners
func (l *Logkit) DeleteRunners(opts *BatchRunnersOptions) error {
	op := newOperation(opDeleteRunners)
	data, err := json.Marshal(opts.RunnerConds)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

type GrokData struct {
	Log           string `json:"log"`
	Pattern       string `json:"pattern"`
	CustomPattern string `json:"custom_pattern"`
}

type GrokCheckOptions struct {
	PandoraToken

	*GrokData
}

// GrokCheck 用于测试 Gork 模式
func (l *Logkit) GrokCheck(opts *GrokCheckOptions) (map[string]interface{}, error) {
	if opts.GrokData == nil {
		return nil, errors.New("field 'GrokData' is nil")
	}

	op := newOperation(opGrokCheck)
	data, err := json.Marshal(opts.GrokData)
	if err != nil {
		return nil, err
	}

	var resp map[string]interface{}
	req := l.newRequest(op, opts.Token, resp)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return resp, req.Send()
}
