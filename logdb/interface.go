package logdb

import (
	"github.com/qiniu/pandora-go-sdk/base"
)

type LogdbAPI interface {
	CreateRepo(*CreateRepoInput) error

	GetRepo(*GetRepoInput) (*GetRepoOutput, error)

	ListRepos(*ListReposInput) (*ListReposOutput, error)

	DeleteRepo(*DeleteRepoInput) error

	UpdateRepo(*UpdateRepoInput) error

	SendLog(*SendLogInput) (*SendLogOutput, error)

	QueryLog(*QueryLogInput) (*QueryLogOutput, error)

	QueryHistogramLog(*QueryHistogramLogInput) (*QueryHistogramLogOutput, error)

	MakeToken(*base.TokenDesc) (string, error)
}
