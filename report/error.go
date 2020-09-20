package report

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
	case "E8002":
		err.ErrorType = reqerr.ErrDBNameInvalidError
	case "E8003":
		err.ErrorType = reqerr.ErrInvalidSqlError
	case "E8005":
		err.ErrorType = reqerr.ErrInvalidParameterError
	case "E8006":
		err.ErrorType = reqerr.ErrDBNotFoundError
	case "E8007":
		err.ErrorType = reqerr.ErrTableNotFoundError
	case "E9001":
		err.ErrorType = reqerr.InternalServerError
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
