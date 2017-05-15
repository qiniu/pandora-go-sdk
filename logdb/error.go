package logdb

import (
	"fmt"

	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

const errCodePrefixLen = 5

type errBuilder struct{}

func (e errBuilder) Build(msg, text, reqId string, code int) error {

	err := reqerr.New(msg, text, reqId, code)
	if len(msg) <= errCodePrefixLen {
		return err
	}
	errId := msg[:errCodePrefixLen]

	switch errId {
	case "E8111":
		err.ErrorType = reqerr.NoSuchRepoError
	case "E8112":
		err.ErrorType = reqerr.RepoAlreadyExistsError
	case "E8201", "E8202", "E8203", "E8204", "E8205", "E8206", "E8207", "E8208", "E8209":
		err.ErrorType = reqerr.InvalidSliceArgumentError
	case "E8004":
		err.ErrorType = reqerr.InternalServerError
	case "E8104":
		err.ErrorType = reqerr.UnmatchedSchemaError
	default:
		if code == 401 {
			err.Message = fmt.Sprintf("unauthorized. Please check the local time to ensure the consistent with the server time, if you are using the token, please make sure that token has not expired.")
			err.ErrorType = reqerr.UnauthorizedError
		}
	}

	return err
}
