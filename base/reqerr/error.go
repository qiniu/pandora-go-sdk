package reqerr

import (
	"fmt"
)

const (
	DefaultRequestError int = iota
	InvalidArgs
	NoSuchRepoError
	RepoAlreadyExistsError
	InvalidSliceArgumentError
	UnmatchedSchemaError
	UnauthorizedError
	InternalServerError
	NoSuchGroupError
	GroupAlreadyExistsError
	NoSuchTransformError
	TransformAlreadyExistsError
	NoSuchExportError
	ExportAlreadyExistsError
	NoSuchPluginError
	PluginAlreadyExistsError
	RepoCascadingError
	RepoInCreatingError
	InvalidTransformSpecError
	InvalidExportSpecError
	NoSuchRetentionError
	SeriesAlreadyExistsError
	NoSuchSeriesError
	InvalidSeriesNameError
	InvalidViewNameError
	InvalidViewSqlError
	ViewFuncNotSupportError
	NoSuchViewError
	ViewAlreadyExistsError
	InvalidViewStatementError
	PointsNotInSameRetentionError
	TimestampTooFarFromNowError
	InvalidQuerySql
	QueryInterruptError
	ExecuteSqlError
	EntityTooLargeError
	InvalidDataSchemaError
)

type ErrBuilder interface {
	Build(message, rawText, reqId string, statusCode int) error
}

func NewInvalidArgs(name, message string) *RequestError {
	return &RequestError{
		Message:   fmt.Sprintf("Invalid args, argName: %s, reason: %s", name, message),
		ErrorType: InvalidArgs,
	}
}

type RequestError struct {
	Message    string `json:"error"`
	StatusCode int    `json:"-"`
	RequestId  string `json:"-"`
	RawMessage string `json:"-"`
	ErrorType  int    `json:"-"`
}

func New(message, rawText, reqId string, statusCode int) *RequestError {
	return &RequestError{
		Message:    message,
		StatusCode: statusCode,
		RequestId:  reqId,
		RawMessage: rawText,
		ErrorType:  DefaultRequestError,
	}
}

func (r RequestError) Error() string {
	return fmt.Sprintf("pandora error: StatusCode=%d, ErrorMessage=%s, RequestId=%s", r.StatusCode, r.Message, r.RequestId)
}
