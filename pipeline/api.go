package pipeline

import (
	"net/url"
	"os"

	. "github.com/qiniu/pandora-go-sdk/base"
)

func (c *Pipeline) CreateGroup(input *CreateGroupInput) (err error) {
	op := c.newOperation(OpCreateGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateGroup(input *UpdateGroupInput) (err error) {
	op := c.newOperation(OpUpdateGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) StartGroupTask(input *StartGroupTaskInput) (err error) {
	op := c.newOperation(OpStartGroupTask, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) StopGroupTask(input *StopGroupTaskInput) (err error) {
	op := c.newOperation(OpStopGroupTask, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) ListGroups(input *ListGroupsInput) (output *ListGroupsOutput, err error) {
	op := c.newOperation(OpListGroups)

	output = &ListGroupsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetGroup(input *GetGroupInput) (output *GetGroupOutput, err error) {
	op := c.newOperation(OpGetGroup, input.GroupName)

	output = &GetGroupOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) DeleteGroup(input *DeleteGroupInput) (err error) {
	op := c.newOperation(OpDeleteGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateRepo(input *CreateRepoInput) (err error) {
	op := c.newOperation(OpCreateRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) CreateRepoFromDSL(input *CreateRepoDSLInput) (err error) {
	schemas, err := toSchema(input.DSL, 0)
	if err != nil {
		return
	}
	return c.CreateRepo(&CreateRepoInput{
		PipelineToken: input.PipelineToken,
		RepoName:      input.RepoName,
		Region:        input.Region,
		GroupName:     input.GroupName,
		Schema:        schemas,
	})
}

func (c *Pipeline) UpdateRepo(input *UpdateRepoInput) (err error) {
	op := c.newOperation(OpUpdateRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) GetRepo(input *GetRepoInput) (output *GetRepoOutput, err error) {
	op := c.newOperation(OpGetRepo, input.RepoName)

	output = &GetRepoOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) ListRepos(input *ListReposInput) (output *ListReposOutput, err error) {
	op := c.newOperation(OpListRepos)

	output = &ListReposOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) DeleteRepo(input *DeleteRepoInput) (err error) {
	op := c.newOperation(OpDeleteRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) PostData(input *PostDataInput) (err error) {
	op := c.newOperation(OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetBufferBody(input.Points.Buffer())
	req.SetHeader(HTTPHeaderContentType, ContentTypeText)
	return req.Send()
}

func (c *Pipeline) PostDataFromFile(input *PostDataFromFileInput) (err error) {
	op := c.newOperation(OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	file, err := os.Open(input.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	req.SetReaderBody(file)
	req.SetHeader(HTTPHeaderContentType, ContentTypeText)
	return req.Send()
}

func (c *Pipeline) PostDataFromReader(input *PostDataFromReaderInput) (err error) {
	op := c.newOperation(OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetReaderBody(input.Reader)
	req.SetHeader(HTTPHeaderContentType, ContentTypeText)
	return req.Send()
}

func (c *Pipeline) PostDataFromBytes(input *PostDataFromBytesInput) (err error) {
	op := c.newOperation(OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetBufferBody(input.Buffer)
	req.SetHeader(HTTPHeaderContentType, ContentTypeText)
	return req.Send()
}

func (c *Pipeline) UploadPlugin(input *UploadPluginInput) (err error) {
	op := c.newOperation(OpUploadPlugin, input.PluginName)

	req := c.newRequest(op, input.Token, nil)
	req.EnableContentMD5d()
	req.SetBufferBody(input.Buffer.Bytes())
	req.SetHeader(HTTPHeaderContentType, ContentTypeJar)
	return req.Send()
}

func (c *Pipeline) UploadPluginFromFile(input *UploadPluginFromFileInput) (err error) {
	op := c.newOperation(OpUploadPlugin, input.PluginName)

	req := c.newRequest(op, input.Token, nil)
	req.EnableContentMD5d()

	file, err := os.Open(input.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	req.SetReaderBody(file)
	req.SetHeader(HTTPHeaderContentType, ContentTypeJar)
	return req.Send()
}

func (c *Pipeline) ListPlugins(input *ListPluginsInput) (output *ListPluginsOutput, err error) {
	op := c.newOperation(OpListPlugins)

	output = &ListPluginsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetPlugin(input *GetPluginInput) (output *GetPluginOutput, err error) {
	op := c.newOperation(OpGetPlugin, input.PluginName)

	output = &GetPluginOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeletePlugin(input *DeletePluginInput) (err error) {
	op := c.newOperation(OpDeletePlugin, input.PluginName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateTransform(input *CreateTransformInput) (err error) {
	op := c.newOperation(OpCreateTransform, input.SrcRepoName, input.TransformName, input.DestRepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input.Spec); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateTransform(input *UpdateTransformInput) (err error) {
	op := c.newOperation(OpUpdateTransform, input.SrcRepoName, input.TransformName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input.Spec); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListTransforms(input *ListTransformsInput) (output *ListTransformsOutput, err error) {
	op := c.newOperation(OpListTransforms, input.RepoName)

	output = &ListTransformsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetTransform(input *GetTransformInput) (output *GetTransformOutput, err error) {
	op := c.newOperation(OpGetTransform, input.RepoName, input.TransformName)

	output = &GetTransformOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteTransform(input *DeleteTransformInput) (err error) {
	op := c.newOperation(OpDeleteTransform, input.RepoName, input.TransformName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateExport(input *CreateExportInput) (err error) {
	op := c.newOperation(OpCreateExport, input.RepoName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListExports(input *ListExportsInput) (output *ListExportsOutput, err error) {
	op := c.newOperation(OpListExports, input.RepoName)

	output = &ListExportsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetExport(input *GetExportInput) (output *GetExportOutput, err error) {
	op := c.newOperation(OpGetExport, input.RepoName, input.ExportName)

	output = &GetExportOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteExport(input *DeleteExportInput) (err error) {
	op := c.newOperation(OpDeleteExport, input.RepoName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateDatasource(input *CreateDatasourceInput) (err error) {
	op := c.newOperation(OpCreateDatasource, input.DatasourceName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListDatasources() (output *ListDatasourcesOutput, err error) {
	op := c.newOperation(OpListDatasources)

	output = &ListDatasourcesOutput{}
	req := c.newRequest(op, "", &output)
	return output, req.Send()
}

func (c *Pipeline) GetDatasource(input *GetDatasourceInput) (output *GetDatasourceOutput, err error) {
	op := c.newOperation(OpGetDatasource, input.DatasourceName)

	output = &GetDatasourceOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteDatasource(input *DeleteDatasourceInput) (err error) {
	op := c.newOperation(OpDeleteDatasource, input.DatasourceName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateJob(input *CreateJobInput) (err error) {
	op := c.newOperation(OpCreateJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListJobs(input *ListJobsInput) (output *ListJobsOutput, err error) {
	query := ""
	values := url.Values{}
	if input.SrcJobName != "" {
		values.Set("srcJob", input.SrcJobName)
	}
	if input.SrcDatasourceName != "" {
		values.Set("srcDatasource", input.SrcDatasourceName)
	}
	if len(values) != 0 {
		query = "?" + values.Encode()
	}
	op := c.newOperation(OpListJobs, query)

	output = &ListJobsOutput{}
	req := c.newRequest(op, "", &output)
	return output, req.Send()
}

func (c *Pipeline) GetJob(input *GetJobInput) (output *GetJobOutput, err error) {
	op := c.newOperation(OpGetJob, input.JobName)

	output = &GetJobOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteJob(input *DeleteJobInput) (err error) {
	op := c.newOperation(OpDeleteJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) StartJob(input *StartJobInput) (err error) {
	op := c.newOperation(OpStartJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) GetJobHistory(input *GetJobHistoryInput) (output *GetJobHistoryOutput, err error) {
	op := c.newOperation(OpGetJobHistory, input.JobName)

	output = &GetJobHistoryOutput{}
	req := c.newRequest(op, input.Token, nil)
	return output, req.Send()
}

func (c *Pipeline) StopJob(input *StopJobInput) (err error) {
	op := c.newOperation(OpStopJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateJobExport(input *CreateJobExportInput) (err error) {
	op := c.newOperation(OpCreateJobExport, input.JobName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListJobExports(input *ListJobExportsInput) (output *ListJobExportsOutput, err error) {
	op := c.newOperation(OpListJobExports, input.JobName)

	output = &ListJobExportsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetJobExport(input *GetJobExportInput) (output *GetJobExportOutput, err error) {
	op := c.newOperation(OpGetJobExport, input.JobName, input.ExportName)

	output = &GetJobExportOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteJobExport(input *DeleteJobExportInput) (err error) {
	op := c.newOperation(OpDeleteJobExport, input.JobName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) RetrieveSchema(input *RetrieveSchemaInput) (output *RetrieveSchemaOutput, err error) {
	op := c.newOperation(OpRetrieveSchema)

	output = &RetrieveSchemaOutput{}
	req := c.newRequest(op, input.Token, &output)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return output, req.Send()
}

func (c *Pipeline) MakeToken(desc *TokenDesc) (string, error) {
	return MakeTokenInternal(c.Config.Ak, c.Config.Sk, desc)
}
