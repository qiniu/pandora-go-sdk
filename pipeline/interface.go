package pipeline

import (
	"github.com/qiniu/pandora-go-sdk/base"
)

type PipelineAPI interface {
	AutoExportToLogDB(*AutoExportToLogDBInput) error

	CreateGroup(*CreateGroupInput) error

	UpdateGroup(*UpdateGroupInput) error

	StartGroupTask(*StartGroupTaskInput) error

	StopGroupTask(*StopGroupTaskInput) error

	ListGroups(*ListGroupsInput) (*ListGroupsOutput, error)

	GetGroup(*GetGroupInput) (*GetGroupOutput, error)

	DeleteGroup(*DeleteGroupInput) error

	CreateRepo(*CreateRepoInput) error

	CreateRepoFromDSL(*CreateRepoDSLInput) error

	UpdateRepo(*UpdateRepoInput) error

	GetRepo(*GetRepoInput) (*GetRepoOutput, error)

	ListRepos(*ListReposInput) (*ListReposOutput, error)

	DeleteRepo(*DeleteRepoInput) error

	PostData(*PostDataInput) error

	PostDataSchemaFree(input *SchemaFreeInput) (map[string]RepoSchemaEntry, error)

	PostDataFromFile(*PostDataFromFileInput) error

	PostDataFromReader(*PostDataFromReaderInput) error

	PostDataFromBytes(*PostDataFromBytesInput) error

	UploadPlugin(*UploadPluginInput) error

	UploadPluginFromFile(*UploadPluginFromFileInput) error

	ListPlugins(*ListPluginsInput) (*ListPluginsOutput, error)

	GetPlugin(*GetPluginInput) (*GetPluginOutput, error)

	DeletePlugin(*DeletePluginInput) error

	CreateTransform(*CreateTransformInput) error

	UpdateTransform(*UpdateTransformInput) error

	GetTransform(*GetTransformInput) (*GetTransformOutput, error)

	ListTransforms(*ListTransformsInput) (*ListTransformsOutput, error)

	DeleteTransform(*DeleteTransformInput) error

	CreateExport(*CreateExportInput) error

	UpdateExport(*UpdateExportInput) error

	GetExport(*GetExportInput) (*GetExportOutput, error)

	ListExports(*ListExportsInput) (*ListExportsOutput, error)

	DeleteExport(*DeleteExportInput) error

	CreateDatasource(*CreateDatasourceInput) error

	GetDatasource(*GetDatasourceInput) (*GetDatasourceOutput, error)

	ListDatasources() (*ListDatasourcesOutput, error)

	DeleteDatasource(*DeleteDatasourceInput) error

	CreateJob(*CreateJobInput) error

	GetJob(*GetJobInput) (*GetJobOutput, error)

	ListJobs(*ListJobsInput) (*ListJobsOutput, error)

	DeleteJob(*DeleteJobInput) error

	StartJob(*StartJobInput) error

	StopJob(*StopJobInput) error

	GetJobHistory(*GetJobHistoryInput) (*GetJobHistoryOutput, error)

	StopJobBatch(*StopJobBatchInput) (*StopJobBatchOutput, error)

	RerunJobBatch(*RerunJobBatchInput) (*RerunJobBatchOutput, error)

	CreateJobExport(*CreateJobExportInput) error

	GetJobExport(*GetJobExportInput) (*GetJobExportOutput, error)

	ListJobExports(*ListJobExportsInput) (*ListJobExportsOutput, error)

	DeleteJobExport(*DeleteJobExportInput) error

	RetrieveSchema(*RetrieveSchemaInput) (*RetrieveSchemaOutput, error)

	MakeToken(*base.TokenDesc) (string, error)

	GetDefault(RepoSchemaEntry) interface{}

	GetUpdateSchemas(string) (map[string]RepoSchemaEntry, error)

	UploadUdf(input *UploadUdfInput) (err error)

	UploadUdfFromFile(input *UploadUdfFromFileInput) (err error)

	PutUdfMeta(input *PutUdfMetaInput) (err error)

	DeleteUdf(input *DeleteUdfInfoInput) (err error)

	ListUdfs(input *ListUdfsInput) (output *ListUdfsOutput, err error)

	RegisterUdfFunction(input *RegisterUdfFunctionInput) (err error)

	DeRegisterUdfFunction(input *DeregisterUdfFunctionInput) (err error)

	ListUdfFunctions(input *ListUdfFunctionsInput) (output *ListUdfFunctionsOutput, err error)

	Close() error
}
