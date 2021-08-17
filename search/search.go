package search

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/qiniu/pandora-go-sdk/v2/auth"
	"github.com/qiniu/pandora-go-sdk/v2/client"
	"github.com/qiniu/pandora-go-sdk/v2/conf"
	"github.com/qiniu/pandora-go-sdk/v2/internal/log"
)

// SearchManager 提供了查询服务
type SearchManager struct {
	Client *client.Client
	Mac    *auth.Credentials
	Cfg    *conf.Config
}

func NewSearchManager(mac *auth.Credentials, cfg *conf.Config) *SearchManager {
	if cfg == nil {
		cfg = &conf.Config{}
	}

	return &SearchManager{
		Client: &client.DefaultClient,
		Mac:    mac,
		Cfg:    cfg,
	}
}

const SearchModeFast = "fast"
const SearchModeSmart = "smart"
const SearchModeDetail = "detailed"
const CollectSizeAllFetch = -1

type SearchParam struct {
	// 查询SPL
	Query string `json:"query,omitempty"`

	// 查询起始时间，如果spl里面指定了这个时间，以查询语句中优先
	StartTime int64 `json:"startTime,omitempty"`

	// 查询结束时间，如果spl里面指定了这个时间，以查询语句中优先
	EndTime int64 `json:"endTime,omitempty"`

	// 搜索模式 fast/smart/detailed
	Mode string `json:"hosts,omitempty"`

	// 是否在未搜索完成时候预览数据
	Preview bool `json:"preview,omitempty"`

	// 限制获取数据大小，-1 代表获取全部
	CollectSize int64 `json:"collectSize,omitempty"`
}

type SearchJob struct {
	// 搜索任务id
	Id string `json:"id,omitempty"`
}

type JobInfo struct {
	// 搜索状态，0代表搜索进行中，1代表搜索结束
	Process int8 `json:"process,omitempty"`

	// 搜索持续时间，毫秒单位
	Duration int64 `json:"duration,omitempty"`

	// 返回命中结果总数
	EventSize int64 `json:"eventSize,omitempty"`

	// 是否SPL transform命令，一般为统计命令结果，如果为true，则应该轮询获得 results 接口结果
	IsResult bool `json:"isResult,omitempty"`

	// 是否SPL export命令，一般为导出结果，如果为true，则应该轮询等待导出结果
	IsExport bool `json:"isExport,omitempty"`

	// results 结果数量
	ResultSize int64 `json:"resultSize,omitempty"`

	// 搜索过程中扫描到的数据条数
	ScanSize int64 `json:"scanSize,omitempty"`
}

func NewSimpleSearchParam(query string, start, end time.Time) *SearchParam {
	return NewSearchParam(query,
		SearchModeFast,
		start.UnixNano()/int64(time.Millisecond),
		end.UnixNano()/int64(time.Millisecond),
		CollectSizeAllFetch,
		false,
	)
}

func NewSearchParam(query, mode string, start, end, size int64, preview bool) *SearchParam {
	return &SearchParam{
		Query:       query,
		Mode:        mode,
		StartTime:   start,
		EndTime:     end,
		CollectSize: size,
		Preview:     preview,
	}
}

/// 创建搜索任务，返回任务ID
func (m *SearchManager) CreateSearchJob(param *SearchParam) (ret SearchJob, err error) {
	err = m.Client.CredentialedCallWithJson(context.Background(), m.Mac, auth.PandoraToken, &ret, "POST", m.Cfg.GetReqUrl()+"/jobs", nil, param)
	return
}

/// 根据任务ID，获取任务运行信息
func (m *SearchManager) GetJobInfo(job *SearchJob) (ret JobInfo, err error) {
	reqUrl := m.Cfg.GetReqUrl() + fmt.Sprintf("/jobs/%s", job.Id)
	err = m.Client.CredentialedCall(context.Background(), m.Mac, auth.PandoraToken, &ret, "GET", reqUrl, nil)
	return
}

type JobResults struct {
	Fields []ResultField `json:"fields,omitempty"`
	Rows   [][]interface{}
}

type ResultField struct {
	BucketIndex int    `json:"bucketIndex,omitempty"`
	Name        string `json:"name,omitempty"`
	Flag        string `json:"flag,omitempty"`
}

/// 获取任务运行结果
func (m *SearchManager) GetJobResult(job *SearchJob) (ret JobResults, err error) {
	reqUrl := m.Cfg.GetReqUrl() + fmt.Sprintf("/jobs/%s/results", job.Id)
	err = m.Client.CredentialedCall(context.Background(), m.Mac, auth.PandoraToken, &ret, "GET", reqUrl, nil)
	return
}

type SearchMapping struct {
	// Schema 信息 {"fieldName": {"type": "double|long|time|string"}}
	Mapping map[string]map[string]string
}

/// 获取SPL 的返回结果Schema
func (m *SearchManager) GetQueryMapping(param *SearchParam) (ret SearchMapping, err error) {
	reqUrl := m.Cfg.GetReqUrl() +
		fmt.Sprintf("/mapping?startTime=%d&endTime=%d&query=%s", param.StartTime, param.EndTime, url.QueryEscape(param.Query))
	err = m.Client.CredentialedCall(context.Background(), m.Mac, auth.PandoraToken, &ret, "GET", reqUrl, nil)
	return
}

/// 杀死未完成任务
func (m *SearchManager) KillSearchJob(job *SearchJob) (err error) {
	reqUrl := m.Cfg.GetReqUrl() + fmt.Sprintf("/jobs/%s/stop", job.Id)
	err = m.Client.CredentialedCall(context.Background(), m.Mac, auth.PandoraToken, nil, "PUT", reqUrl, nil)
	return
}

/// 创建搜索任务，并且轮询等待结果，如果超时则杀死任务并且退出
/// pollInterval: 轮询周期
/// timeout: 轮询等待超时
func (m *SearchManager) CreateAndWaitForQueryResults(param *SearchParam, pollInterval, timeout time.Duration) (job SearchJob, jobInfo JobInfo, jobResults JobResults, err error) {
	start := time.Now()
	job, err = m.CreateSearchJob(param)
	if err != nil {
		return
	}
	for {
		jobInfo, err = m.GetJobInfo(&job)
		if err != nil {
			break
		}
		if jobInfo.Process == 1 {
			jobResults, err = m.GetJobResult(&job)
			break
		}
		if time.Now().After(start.Add(timeout)) {
			err = fmt.Errorf("timeout: waiting %f seconds for job %s, spl is [%d - %d] : %s",
				timeout.Seconds(), job.Id, param.StartTime, param.EndTime, param.Query)
			err2 := m.KillSearchJob(&job)
			if err2 != nil {
				log.Warn(fmt.Sprintf("kill job %s failed %s", job.Id, err.Error()))
			}
			break
		}
		time.Sleep(pollInterval)
	}
	return
}
