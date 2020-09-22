package logkit

import (
	"fmt"
	"strings"

	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

const errCodePrefixLen = 5

var builder ErrBuilder

type ErrBuilder struct{}

func (e ErrBuilder) Build(msg, text, reqID string, code int) error {

	err := reqerr.New(msg, text, reqID, code)
	if len(msg) <= errCodePrefixLen {
		return err
	}
	err.Component = "logkit"
	errID := msg[:errCodePrefixLen]
	if strings.Contains(errID, ":") {
		spls := strings.Split(errID, ":")
		if len(spls) > 0 {
			errID = spls[0]
		}
	}
	switch errID {
	case "E1001":
		err.ErrorType = reqerr.ErrRequestBodyInvalid
	case "E1002":
		err.ErrorType = reqerr.ErrInvalidArgs
	case "E1005":
		err.ErrorType = reqerr.ErrJobInfoInvalid
	case "E1006":
		err.ErrorType = reqerr.ErrAgentRegister
	case "E1007":
		err.ErrorType = reqerr.ErrAgentReportJobStates
	case "E1009":
		err.ErrorType = reqerr.ErrUnknownAction
	case "E1010":
		err.ErrorType = reqerr.ErrGetUserInformation
	case "E1011":
		err.ErrorType = reqerr.ErrGetPagingInfo
	case "E1012":
		err.ErrorType = reqerr.ErrGetAgentInfo
	case "E1013":
		err.ErrorType = reqerr.ErrGetAgentIdList
	case "E1015":
		err.ErrorType = reqerr.ErrGetRunnerInfo
	case "E1017":
		err.ErrorType = reqerr.ErrRemoveAgentInfo
	case "E1018":
		err.ErrorType = reqerr.ErrRemoveRunner
	case "E1019":
		err.ErrorType = reqerr.ErrRemoveJobStates
	case "E1020":
		err.ErrorType = reqerr.ErrUpdateAgentInfo
	case "E1021":
		err.ErrorType = reqerr.ErrAgentReportMetrics
	case "E1023":
		err.ErrorType = reqerr.ErrGetVersionConfigItem
	case "E1024":
		err.ErrorType = reqerr.ErrNoValidAgentsFound
	case "E1025":
		err.ErrorType = reqerr.ErrScheduleAgent
	case "E1026":
		err.ErrorType = reqerr.ErrNoSuchAgent
	case "E1027":
		err.ErrorType = reqerr.ErrGetMachineInfo
	case "E1050":
		err.ErrorType = reqerr.ErrGetAgents
	case "E1029":
		err.ErrorType = reqerr.ErrDoLogDBMSearch
	case "E1030":
		err.ErrorType = reqerr.ErrGetJobJnfos
	case "E1031":
		err.ErrorType = reqerr.ErrGetJobStates
	case "E1032":
		err.ErrorType = reqerr.ErrGetUserToken
	case "E1034":
		err.ErrorType = reqerr.ErrSystemNotSupport
	case "E1035":
		err.ErrorType = reqerr.ErrGetSenderPandora
	case "E1037":
		err.ErrorType = reqerr.ErrStatusDecodeBase64
	case "E1038":
		err.ErrorType = reqerr.ErrGetGrokCheck
	case "E1039":
		err.ErrorType = reqerr.ErrParamsCheck
	case "E1040":
		err.ErrorType = reqerr.ErrGetTagList
	case "E1041":
		err.ErrorType = reqerr.ErrGetConfigList
	case "E1042":
		err.ErrorType = reqerr.ErrInsertConfigs
	case "E1043":
		err.ErrorType = reqerr.ErrAssignConfigs
	case "E1048":
		err.ErrorType = reqerr.ErrGetRunnerList
	case "E1049":
		err.ErrorType = reqerr.ErrUpdateAssignConfigs
	case "E1055":
		err.ErrorType = reqerr.ErrRawDataSize
	case "E1056":
		err.ErrorType = reqerr.ErrHeadPattern
	case "E1057":
		err.ErrorType = reqerr.ErrConfig
	case "E1058":
		err.ErrorType = reqerr.ErrUnmarshal
	case "E1059":
		err.ErrorType = reqerr.ErrLogParser
	case "E1060":
		err.ErrorType = reqerr.ErrTransformer
	case "E1061":
		err.ErrorType = reqerr.ErrSender
	case "E1062":
		err.ErrorType = reqerr.ErrRouter
	case "E1065":
		err.ErrorType = reqerr.ErrUpdateTags
	case "E1066":
		err.ErrorType = reqerr.ErrAgentInfoNotFound
	case "E1067":
		err.ErrorType = reqerr.ErrGetAgentRelease
	case "E1068":
		err.ErrorType = reqerr.ErrJobRelease
	case "E1069":
		err.ErrorType = reqerr.ErrAgentsDisconnect
	case "E1070":
		err.ErrorType = reqerr.ErrUpdateConfigs
	case "E1071":
		err.ErrorType = reqerr.ErrRemoveConfigs
	case "E1072":
		err.ErrorType = reqerr.ErrUpdateRunners
	case "E1074":
		err.ErrorType = reqerr.ErrAssignTags
	case "E1075":
		err.ErrorType = reqerr.ErrAddTags
	case "E1076":
		err.ErrorType = reqerr.ErrRemoveTags
	case "E1078":
		err.ErrorType = reqerr.ErrNotFoundRecord
	case "E1079":
		err.ErrorType = reqerr.ErrExistRecord
	case "E1088":
		err.ErrorType = reqerr.ErrDoLogDBJob
	case "E1089":
		err.ErrorType = reqerr.ErrDoLogDBAnalysis
	case "E1090":
		err.ErrorType = reqerr.ErrTimeStamp
	case "E1080":
		err.ErrorType = reqerr.ErrNoRunnerInfo
	case "E1081":
		err.ErrorType = reqerr.ErrMachineMetricNotOn
	case "E1082":
		err.ErrorType = reqerr.ErrDeleteRunner
	case "E1083":
		err.ErrorType = reqerr.ErrAddRunner
	case "E1084":
		err.ErrorType = reqerr.ErrDisableDeleteMetrics
	case "E1085":
		err.ErrorType = reqerr.ErrDisablePostMetrics
	case "E1086":
		err.ErrorType = reqerr.ErrUnprocessableEntity
	case "E1087":
		err.ErrorType = reqerr.ErrDisablePostMetricsLogdb
	case "E18669":
		err.ErrorType = reqerr.ErrAccountArrearsProtection
	case "E18670":
		err.ErrorType = reqerr.ErrAccountFrozen
	default:
		if code == 401 {
			err.Message = fmt.Sprintf("unauthorized: %v. 1. Please check your qiniu access_key and secret_key are both correct and you're authorized qiniu pandora user. 2. Please check the local time to ensure the consistent with the server time. 3. If you are using the token, please make sure that token has not expired.", msg)
			err.ErrorType = reqerr.UnauthorizedError
		}
	}
	return err

}
