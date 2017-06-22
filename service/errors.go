package service

import "github.com/qiniu/pandora-go-sdk/base/reqerr"

//TODO： 测试 series 存在的时候，是不是这样一个错误
func IsExistRepoError(err error) bool {
	reqErr, ok := err.(*reqerr.RequestError)
	if !ok {
		return false
	}
	if reqErr.ErrorType == reqerr.RepoAlreadyExistsError {
		return true
	}
	return false
}
