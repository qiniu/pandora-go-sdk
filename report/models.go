package report

import "github.com/qiniu/pandora-go-sdk/base/reqerr"

type ReportToken struct {
	Token string `json:"-"`
}

type UserActivateInput struct {
	ReportToken
}

//database related
type CreateDatabaseInput struct {
	ReportToken
	DatabaseName string
	Region       string `json:"region"`
}

func (r *CreateDatabaseInput) Validate() (err error) {

	if r.Region == "" {
		return reqerr.NewInvalidArgs("Region", "region should not be empty")
	}
	return
}

type ListDatabasesInput struct {
	ReportToken
}

type ListDatabasesOutput []string

type DeleteDatabaseInput struct {
	ReportToken
	DatabaseName string
}

//Table related
type CreateTableInput struct {
	ReportToken
	DatabaseName string
	TableName    string
	CMD          string
}

type UpdateTableInput CreateTableInput

type ListTablesInput struct {
	ReportToken
	DatabaseName string
}

type ListTablesOutput []string

type DeleteTableInput struct {
	DatabaseName string
	TableName    string
	ReportToken
}
