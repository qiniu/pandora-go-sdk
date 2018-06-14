package logkit

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/go-querystring/query"

	"github.com/qiniu/pandora-go-sdk/base"
	. "github.com/qiniu/pandora-go-sdk/base/models"
)

type Tag struct {
	Name        string   `json:"name"`
	AgentIDs    []string `json:"agent_ids"`
	ConfigNames []string `json:"config_names"`
	DialNames   []string `json:"dial_names"`
	Note        string   `json:"note"`
}

type GetTagsOptions struct {
	PandoraToken `url:"-"`

	Name          string `url:"name"`
	IncludeAgents bool   `url:"agents"`
	Sort          string `url:"sort"`
	Order         string `url:"order"`
	Search        string `url:"search"`

	Page int `url:"page"`
	Size int `url:"size"`
}

// GetTags 返回符合条件的 tags、agents（可选）信息以及可获取的总数
func (l *Logkit) GetTags(opts *GetTagsOptions) ([]*Tag, map[string]interface{}, int, error) {
	vals, err := query.Values(opts)
	if err != nil {
		return nil, nil, 0, err
	}
	op := newOperation(opGetTags, vals.Encode())
	resp := &struct {
		Tags      []*Tag                 `json:"tags"`
		Agents    map[string]interface{} `json:"agents"`
		TotalSize int                    `json:"totalSize"`
	}{}
	return resp.Tags, resp.Agents, resp.TotalSize, l.newRequest(op, opts.Token, resp).Send()
}

type NewTagOptions struct {
	PandoraToken

	*Tag
}

// NewTag 添加新的 tag
func (l *Logkit) NewTag(opts *NewTagOptions) error {
	if opts.Tag == nil {
		return errors.New("field 'Tag' is nil")
	}

	op := newOperation(opNewTag, opts.Name)
	data, err := json.Marshal(opts.Tag)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

type UpdateTagNoteOptions struct {
	PandoraToken `json:"-"`

	Name string `json:"-"`
	Note string `json:"note"`
}

// UpdateTagNote 更新 tag 的 note
func (l *Logkit) UpdateTagNote(opts *UpdateTagNoteOptions) error {
	op := newOperation(opUpdateTagNote, opts.Name)
	data, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

type AssignTagAgentsOptions struct {
	PandoraToken `json:"-"`

	TagName  string   `json:"-"`
	AgentIDs []string `json:"agent_ids"`
}

// AssignTagAgents 分配 tag 给指定的 agents
func (l *Logkit) AssignTagAgents(opts *AssignTagAgentsOptions) error {
	op := newOperation(opAssignTagAgents, opts.TagName)
	data, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

type UnassignTagAgentsOptions struct {
	PandoraToken

	TagName  string
	AgentIDs []string
}

// UnassignTagAgents 取消分配到指定 agents 的 tag
func (l *Logkit) UnassignTagAgents(opts *UnassignTagAgentsOptions) error {
	op := newOperation(opUnassignTagAgents, opts.TagName, strings.Join(opts.AgentIDs, ","))
	return l.newRequest(op, opts.Token, nil).Send()
}

type UnassignTagConfigOptions struct {
	PandoraToken

	TagName    string
	ConfigName string
}

// UnassignTagConfig 删除为 tags 分发的 configs
func (l *Logkit) UnassignTagConfig(opts *UnassignTagConfigOptions) error {
	op := newOperation(opUnassignTagAgents, opts.TagName, opts.ConfigName)
	return l.newRequest(op, opts.Token, nil).Send()
}

type DeleteTagOptions struct {
	PandoraToken

	Name string
}

// DeleteTag 删除指定的 tag
func (l *Logkit) DeleteTag(opts *DeleteTagOptions) error {
	op := newOperation(opDeleteTag, opts.Name)
	return l.newRequest(op, opts.Token, nil).Send()
}

type DeleteTagsOptions struct {
	PandoraToken

	Tags []string
}

// DeleteTags 删除指定的 tags
func (l *Logkit) DeleteTags(opts *DeleteTagsOptions) error {
	op := newOperation(opDeleteTags, strings.Join(opts.Tags, ","))
	return l.newRequest(op, opts.Token, nil).Send()
}
