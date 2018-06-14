package logkit

import (
	"encoding/json"
	"strings"

	"github.com/google/go-querystring/query"

	"github.com/qiniu/pandora-go-sdk/base"
	. "github.com/qiniu/pandora-go-sdk/base/models"
)

type CPUInfo struct {
	Model string  `json:"model"`
	Mhz   float64 `json:"mhz"`
	Cache int32   `json:"cache"`
	Cores int32   `json:"cores"`
	Count int     `json:"count"`
}

type NetInter struct {
	Name    string   `json:"name"`
	MTU     int      `json:"mtu"`
	Mac     string   `json:"mac"`
	IP      []string `json:"ip"`
	NetMask []string `json:"mask"`
}

type DiskInfo struct {
	Device string `json:"device"`
	Fstype string `json:"fstype"`
	Mount  string `json:"mount"`
	Size   uint64 `json:"size"`
}

type Machine struct {
	Arch            string     `json:"arch"`
	Hostname        string     `json:"hostname"`
	BootTime        uint64     `json:"boot_time"`
	Kernel          string     `json:"kernel"`
	KernelVersion   string     `json:"kernel_version"`
	Platform        string     `json:"platform"`
	PlatformVersion string     `json:"platform_version"`
	CPU             CPUInfo    `json:"cpu"`
	Memory          uint64     `json:"memory"`
	NetInterface    []NetInter `json:"interface"`
	Disk            []DiskInfo `json:"disk"`
}

type Agent struct {
	ID         string   `json:"id"`
	IP         string   `json:"ip"`
	UID        string   `json:"uid"`
	RemoteAddr string   `json:"remote_addr"`
	Tags       []string `json:"tags"`
	State      string   `json:"state"`
	Version    string   `json:"version"`
	HostName   string   `json:"hostname"`
	MacAddress string   `json:"mac"`
	CreateTime int64    `json:"create_time"`
	Machine    Machine  `json:"machine"`
	Runners    []string `json:"runners"`
}

type AgentState string

const (
	StateRegister AgentState = "register"
	StateOnline   AgentState = "online"
	StateOffline  AgentState = "offline"
)

type SortOrder string

const (
	OrderAscend  SortOrder = "ascend"
	OrderDescend SortOrder = "descend"
)

type GetAgentsOptions struct {
	PandoraToken `url:"-"`

	Tag    string     `url:"tag"`
	ID     string     `url:"id"`
	State  AgentState `url:"state"`
	Sort   string     `url:"sort"`
	Order  SortOrder  `url:"order"`
	Search string     `url:"search"`

	Page int `url:"page"`
	Size int `url:"size"`
}

// GetAgents 返回符合条件的 agents 信息以及可获取的总数
func (l *Logkit) GetAgents(opts *GetAgentsOptions) ([]*Agent, int, error) {
	vals, err := query.Values(opts)
	if err != nil {
		return nil, 0, err
	}
	op := newOperation(opGetAgents, vals.Encode())
	resp := &struct {
		Agents    []*Agent `json:"agentList"`
		TotalSize int      `json:"totalSize"`
	}{}
	return resp.Agents, resp.TotalSize, l.newRequest(op, opts.Token, resp).Send()
}

type DeleteAgentsOptions struct {
	PandoraToken `url:"-"`

	Tag   string     `url:"tag"`
	ID    string     `url:"id"`
	State AgentState `url:"state"`
}

// DeleteAgents 删除符合条件的 agents
func (l *Logkit) DeleteAgents(opts *DeleteAgentsOptions) error {
	vals, err := query.Values(opts)
	if err != nil {
		return err
	}
	op := newOperation(opDeleteAgents, vals.Encode())
	return l.newRequest(op, opts.Token, nil).Send()
}

type BatchDeleteAgentsOptions struct {
	PandoraToken

	IDs []string
}

// BatchDeleteAgents 根据 ID 删除对应的 agents
func (l *Logkit) BatchDeleteAgents(opts *BatchDeleteAgentsOptions) error {
	op := newOperation(opBatchDeleteAgents, strings.Join(opts.IDs, ","))
	return l.newRequest(op, opts.Token, nil).Send()
}

type AssignAgentTagOptions struct {
	PandoraToken

	AgentID string
	Tag     string
}

// AssignAgentTag 分配指定 tag 给 agent
func (l *Logkit) AssignAgentTag(opts *AssignAgentTagOptions) error {
	op := newOperation(opAssignAgentTag, opts.AgentID, opts.Tag)
	return l.newRequest(op, opts.Token, nil).Send()
}

type AssignAgentTagsOptions struct {
	PandoraToken `json:"-"`

	AgentID string   `json:"-"`
	Tags    []string `json:"tags"`
}

// AssignAgentTags 分配指定 tags 给 agent
func (l *Logkit) AssignAgentTags(opts *AssignAgentTagsOptions) error {
	op := newOperation(opAssignAgentTags, opts.AgentID)
	data, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	req := l.newRequest(op, opts.Token, nil)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

// UnassignAgentTag 从 agent 删除指定 tag
func (l *Logkit) UnassignAgentTag(opts *AssignAgentTagOptions) error {
	op := newOperation(opDeleteTagFromAgent, opts.AgentID, opts.Tag)
	return l.newRequest(op, opts.Token, nil).Send()
}

type MatchAgentsOptions struct {
	PandoraToken `json:"-"`

	AgentIDs []string `json:"agent_ids"`
	Tags     []string `json:"tags"`
}

type AgentRelease struct {
	OS           string `json:"os"`
	Arch         string `json:"arch"`
	Hash         string `json:"hash"`
	Message      string `json:"message" `
	Version      string `json:"version"`
	Package      string `json:"package"`
	DownloadURL  string `json:"download_url"`
	VersionStamp int64  `json:"version_stamp"`
}

type GetAgentReleasesOptions struct {
	PandoraToken
}

// GetAgentReleases 返回 agent 版本信息
func (l *Logkit) GetAgentReleases(opts *GetAgentReleasesOptions) ([]*AgentRelease, error) {
	op := newOperation(opGetAgentReleases)
	var releases []*AgentRelease
	return releases, l.newRequest(op, opts.Token, &releases).Send()
}

// UpgradeAgents 升级符合条件的 agents
func (l *Logkit) UpgradeAgents(opts *MatchAgentsOptions) (string, error) {
	op := newOperation(opUpgradeAgents)
	data, err := json.Marshal(opts)
	if err != nil {
		return "", err
	}

	var jobID string
	req := l.newRequest(op, opts.Token, &jobID)
	req.SetBufferBody(data)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return jobID, req.Send()
}
