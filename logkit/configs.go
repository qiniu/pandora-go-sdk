package logkit

import (
	"encoding/json"
	"errors"

	"github.com/google/go-querystring/query"

	"github.com/qiniu/pandora-go-sdk/base"
	. "github.com/qiniu/pandora-go-sdk/base/models"
)

type Config struct {
	Name      string      `json:"name"`
	Tags      []string    `json:"tags"`
	AgentIDs  []string    `json:"agent_ids"`
	Config    interface{} `json:"config"`
	Note      string      `json:"note"`
	Timestamp int64       `json:"timestamp"`
}

type GetConfigsOptions struct {
	PandoraToken `url:"-"`

	Name   string `url:"name"`
	Sort   string `url:"sort"`
	Order  string `url:"order"`
	Search string `url:"search"`

	Page int `url:"page"`
	Size int `url:"size"`
}

// GetConfigs 返回符合条件的 configs 信息以及可获取的总数
func (l *Logkit) GetConfigs(opts *GetConfigsOptions) ([]*Config, int, error) {
	vals, err := query.Values(opts)
	if err != nil {
		return nil, 0, err
	}
	op := newOperation(opGetConfigs, vals.Encode())
	resp := &struct {
		Configs   []*Config `json:"configs"`
		TotalSize int       `json:"totalSize"`
	}{}
	return resp.Configs, resp.TotalSize, l.newRequest(op, opts.Token, resp).Send()
}

type NewConfigOptions struct {
	PandoraToken

	*Config
}

// NewConfig 添加新的 config
func (l *Logkit) NewConfig(opts *NewConfigOptions) error {
	if opts.Config == nil {
		return errors.New("field 'Config' is nil")
	}

	op := newOperation(opNewConfig, opts.Name)
	data, err := json.Marshal(opts.Config)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

type UpdateConfigOptions struct {
	PandoraToken `json:"-"`

	Name   string      `json:"-"`
	Config interface{} `json:"config"`
	Note   string      `json:"note"`
}

// UpdateConfig 更新指定名称的 config
func (l *Logkit) UpdateConfig(opts *UpdateConfigOptions) error {
	if opts.Config == nil {
		return errors.New("field 'Config' is nil")
	}

	op := newOperation(opUpdateConfig, opts.Name)
	data, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

type DeleteConfigsOptions struct {
	PandoraToken

	Names []string
}

// DeleteConfigs 删除指定名称的 configs
func (l *Logkit) DeleteConfigs(opts *DeleteConfigsOptions) error {
	op := newOperation(opDeleteConfigs)
	data, err := json.Marshal(opts.Names)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

type DeleteConfigOptions struct {
	PandoraToken

	Name string
}

// DeleteConfig 删除指定名称的 config
func (l *Logkit) DeleteConfig(opts *DeleteConfigOptions) error {
	op := newOperation(opDeleteConfig, opts.Name)
	return l.newRequest(op, opts.Token, nil).Send()
}

type AssignConfigTagsOptions struct {
	PandoraToken `json:"-"`

	Name string   `json:"-"`
	Tags []string `json:"tags"`
}

// AssignConfigTags 分配指定 tags 给 config
func (l *Logkit) AssignConfigTags(opts *AssignConfigTagsOptions) error {
	op := newOperation(opAssignAgentTags, opts.Name)
	data, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

type AssignConfigAgentsOptions struct {
	PandoraToken `json:"-"`

	Name     string   `json:"-"`
	AgentIDs []string `json:"agent_ids"`
}

// AssignConfigAgents 分配指定 config 到 agents
func (l *Logkit) AssignConfigAgents(opts *AssignConfigAgentsOptions) error {
	op := newOperation(opAssignConfigAgents, opts.Name)
	data, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}
