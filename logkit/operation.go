package logkit

import (
	"fmt"
	"net/http"

	"github.com/qiniu/pandora-go-sdk/base/request"
)

const (
	opGetAgents          = "GetAgents"
	opDeleteAgents       = "DeleteAgents"
	opBatchDeleteAgents  = "BatchDeleteAgents"
	opAssignAgentTag     = "AssignAgentTag"
	opAssignAgentTags    = "AssignAgentTags"
	opDeleteTagFromAgent = "UnassignAgentTag"
	opGetAgentReleases   = "GetAgentReleases"
	opUpgradeAgents      = "UpgradeAgents"

	opGetConfigs         = "GetConfigs"
	opNewConfig          = "NewConfig"
	opUpdateConfig       = "UpdateConfig"
	opDeleteConfigs      = "DeleteConfigs"
	opDeleteConfig       = "DeleteConfig"
	opAssignConfigTags   = "AssignConfigTags"
	opAssignConfigAgents = "AssignConfigAgents"

	opGetMetricsInfo  = "GetMetricsInfo"
	opGetAgentMetrics = "GetAgentMetrics"

	opGetRunners    = "GetRunners"
	opStartRunners  = "StartRunners"
	opStopRunners   = "StopRunners"
	opResetRunners  = "ResetRunners"
	opDeleteRunners = "DeleteRunners"
	opGrokCheck     = "GrokCheck"

	opGetTags           = "GetTags"
	opNewTag            = "NewTag"
	opUpdateTagNote     = "UpdateTagNote"
	opAssignTagAgents   = "AssignTagAgents"
	opUnassignTagAgents = "UnassignTagAgents"
	opUnassignTagConfig = "UnassignTagConfig"
	opDeleteTag         = "DeleteTag"
	opDeleteTags        = "DeleteTags"
)

func newOperation(opName string, args ...interface{}) *request.Operation {
	var method, urlTmpl string
	switch opName {
	case opGetAgents:
		method, urlTmpl = http.MethodGet, "/agents?%s"
	case opDeleteAgents:
		method, urlTmpl = http.MethodDelete, "/agents?%s"
	case opBatchDeleteAgents:
		method, urlTmpl = http.MethodDelete, "/agents/batch?id=%s"
	case opAssignAgentTag:
		method, urlTmpl = http.MethodPost, "/agents/%s/tags/%s"
	case opAssignAgentTags:
		method, urlTmpl = http.MethodPost, "/agents/%s/tags"
	case opDeleteTagFromAgent:
		method, urlTmpl = http.MethodDelete, "/agents/%s/tags/%s"
	case opGetAgentReleases:
		method, urlTmpl = http.MethodGet, "/agents/release"
	case opUpgradeAgents:
		method, urlTmpl = http.MethodPost, "/agents/upgrade"

	case opGetConfigs:
		method, urlTmpl = http.MethodGet, "/configs?%s"
	case opNewConfig:
		method, urlTmpl = http.MethodPost, "/configs/%s"
	case opUpdateConfig:
		method, urlTmpl = http.MethodPut, "/configs/%s/config"
	case opDeleteConfigs:
		method, urlTmpl = http.MethodPost, "/configs/delete"
	case opDeleteConfig:
		method, urlTmpl = http.MethodDelete, "/configs/%s"
	case opAssignConfigTags:
		method, urlTmpl = http.MethodPost, "/configs/%s/tags"
	case opAssignConfigAgents:
		method, urlTmpl = http.MethodPost, "/configs/%s/agents"

	case opGetMetricsInfo:
		method, urlTmpl = http.MethodGet, "/metrics/machine"
	case opGetAgentMetrics:
		method, urlTmpl = http.MethodGet, "/metrics/%s/data?%s"

	case opGetRunners:
		method, urlTmpl = http.MethodGet, "/runners?%s"
	case opStartRunners:
		method, urlTmpl = http.MethodPost, "/runners/start"
	case opStopRunners:
		method, urlTmpl = http.MethodPost, "/runners/stop"
	case opResetRunners:
		method, urlTmpl = http.MethodPost, "/runners/reset"
	case opDeleteRunners:
		method, urlTmpl = http.MethodPost, "/runners/delete"
	case opGrokCheck:
		method, urlTmpl = http.MethodPost, "/grok/check"

	case opGetTags:
		method, urlTmpl = http.MethodGet, "/tags?%s"
	case opNewTag:
		method, urlTmpl = http.MethodPost, "/tags/%s"
	case opUpdateTagNote:
		method, urlTmpl = http.MethodPut, "/tags/%s/note"
	case opAssignTagAgents:
		method, urlTmpl = http.MethodPost, "/tags/%s/agentids"
	case opUnassignTagAgents:
		method, urlTmpl = http.MethodDelete, "/tags/%s/agentids/batch?agentids=%s"
	case opUnassignTagConfig:
		method, urlTmpl = http.MethodDelete, "/tags/%s/configs/%s"
	case opDeleteTag:
		method, urlTmpl = http.MethodDelete, "/tags/%s"
	case opDeleteTags:
		method, urlTmpl = http.MethodDelete, "/tags/batch?tag=%s"
	}

	return &request.Operation{
		Name:   opName,
		Method: method,
		Path:   fmt.Sprintf("/v1/api"+urlTmpl, args...),
	}
}
