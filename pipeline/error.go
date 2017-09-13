package pipeline

import (
	"fmt"

	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

const errCodePrefixLen = 6

type errBuilder struct{}

func (e errBuilder) Build(msg, text, reqId string, code int) error {

	err := reqerr.New(msg, text, reqId, code)
	if len(msg) <= errCodePrefixLen {
		return err
	}
	errId := msg[:errCodePrefixLen]

	switch errId {
	case "E18005":
		err.ErrorType = reqerr.EntityTooLargeError
	case "E18120":
		err.ErrorType = reqerr.NoSuchGroupError
	case "E18218":
		err.ErrorType = reqerr.GroupAlreadyExistsError
	case "E18102":
		err.ErrorType = reqerr.NoSuchRepoError
	case "E18101":
		err.ErrorType = reqerr.RepoAlreadyExistsError
	case "E18202":
		err.ErrorType = reqerr.NoSuchTransformError
	case "E18201":
		err.ErrorType = reqerr.TransformAlreadyExistsError
	case "E18302":
		err.ErrorType = reqerr.NoSuchExportError
	case "E18301":
		err.ErrorType = reqerr.ExportAlreadyExistsError
	case "E18216":
		err.ErrorType = reqerr.NoSuchPluginError
	case "E18217":
		err.ErrorType = reqerr.PluginAlreadyExistsError
	case "E18124":
		err.ErrorType = reqerr.RepoInCreatingError
	case "E18112":
		err.ErrorType = reqerr.RepoCascadingError
	case "E18207", "E18208", "E18209", "E18210", "E18211":
		err.ErrorType = reqerr.InvalidTransformSpecError
	case "E18303":
		err.ErrorType = reqerr.InvalidExportSpecError
	case "E18125", "E18123", "E18111", "E18110", "E18107", "E18104":
		err.ErrorType = reqerr.InvalidDataSchemaError
	case "E18305":
		err.ErrorType = reqerr.ExportSpecRemainUnchanged
	case "E18639":
		err.ErrorType = reqerr.ErrInvalidWorkflowName
	case "E18640":
		err.ErrorType = reqerr.ErrWorkflowAlreadyExists
	case "E18641":
		err.ErrorType = reqerr.ErrNoSuchWorkflow
	case "E18642":
		err.ErrorType = reqerr.ErrWorkflowSpecContent
	case "E18643":
		err.ErrorType = reqerr.ErrWorkflowStatusCannotUpdate
	case "E9000":
		err.ErrorType = reqerr.InternalServerError
	default:
		if code == 401 {
			err.Message = fmt.Sprintf("unauthorized: %v. 1. Please check your qiniu access_key and secret_key are both correct and you're authorized qiniu pandora user. 2. Please check the local time to ensure the consistent with the server time. 3. If you are using the token, please make sure that token has not expired.", msg)
			err.ErrorType = reqerr.UnauthorizedError
		}
	}
	return err

}
