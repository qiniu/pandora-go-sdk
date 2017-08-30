package report

import (
	"github.com/qiniu/pandora-go-sdk/base"
)

type ReportAPI interface {
	ActivateUser(*UserActivateInput) (*UserActivateOutput, error)

	CreateDatabase(*CreateDatabaseInput) error

	ListDatabases(*ListDatabasesInput) (*ListDatabasesOutput, error)

	DeleteDatabase(*DeleteDatabaseInput) error

	CreateTable(*CreateTableInput) error

	UpdateTable(*UpdateTableInput) error

	ListTables(*ListTablesInput) (*ListTablesOutput, error)

	DeleteTable(*DeleteTableInput) error

	MakeToken(*base.TokenDesc) (string, error)
}
