package pipeline

import (
	"bytes"
	"net/url"
	"os"

	"fmt"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	"github.com/qiniu/pandora-go-sdk/logdb"
	"github.com/qiniu/pandora-go-sdk/tsdb"
)

func (c *Pipeline) CreateGroup(input *CreateGroupInput) (err error) {
	op := c.newOperation(base.OpCreateGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateGroup(input *UpdateGroupInput) (err error) {
	op := c.newOperation(base.OpUpdateGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) StartGroupTask(input *StartGroupTaskInput) (err error) {
	op := c.newOperation(base.OpStartGroupTask, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) StopGroupTask(input *StopGroupTaskInput) (err error) {
	op := c.newOperation(base.OpStopGroupTask, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) ListGroups(input *ListGroupsInput) (output *ListGroupsOutput, err error) {
	op := c.newOperation(base.OpListGroups)

	output = &ListGroupsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetGroup(input *GetGroupInput) (output *GetGroupOutput, err error) {
	op := c.newOperation(base.OpGetGroup, input.GroupName)

	output = &GetGroupOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) DeleteGroup(input *DeleteGroupInput) (err error) {
	op := c.newOperation(base.OpDeleteGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateRepo(input *CreateRepoInput) (err error) {
	op := c.newOperation(base.OpCreateRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if input.Region == "" {
		input.Region = c.defaultRegion
	}
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
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

func (c *Pipeline) UpdateRepoWithTSDB(input *UpdateRepoInput, ex ExportDesc) error {
	tags, ok := ex.Spec["tags"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("export tsdb spec tags assert error %v is not map[string]interface{}", ex.Spec["tags"])
	}
	fields, ok := ex.Spec["fields"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("export tsdb spec fields assert error %v is not map[string]interface{}", ex.Spec["fields"])
	}
	for _, v := range input.Schema {
		if input.IsTag(v.Key) {
			tags[v.Key] = v.Key
		} else {
			fields[v.Key] = v.Key
		}
	}
	ex.Spec["tags"] = tags
	ex.Spec["fields"] = fields
	return c.UpdateExport(&UpdateExportInput{
		RepoName:   input.RepoName,
		ExportName: ex.Name,
		Spec:       ex.Spec,
	})
}

func schemaNotIn(key string, schemas []logdb.RepoSchemaEntry) bool {
	for _, v := range schemas {
		if v.Key == key {
			return false
		}
	}
	return true
}

func (c *Pipeline) UpdateRepoWithLogDB(input *UpdateRepoInput, ex ExportDesc) error {
	repoName, ok := ex.Spec["destRepoName"].(string)
	if !ok {
		return fmt.Errorf("export logdb spec destRepoName assert error %v is not string", ex.Spec["destRepoName"])
	}
	docs, ok := ex.Spec["doc"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("export logdb spec doc assert error %v is not map[string]interface{}", ex.Spec["doc"])
	}
	for _, v := range input.Schema {
		docs[v.Key] = v.Key
	}
	logdbAPI, err := c.GetLogDBAPI()
	if err != nil {
		return err
	}
	repoInfo, err := logdbAPI.GetRepo(&logdb.GetRepoInput{RepoName: repoName})
	if err != nil {
		return err
	}
	for _, v := range input.Schema {
		if schemaNotIn(v.Key, repoInfo.Schema) {
			scs := convertSchema2LogDB([]RepoSchemaEntry{v})
			if len(scs) > 0 {
				repoInfo.Schema = append(repoInfo.Schema, scs[0])
			}
			docs[v.Key] = v.Key
		}
	}
	if err = logdbAPI.UpdateRepo(&logdb.UpdateRepoInput{
		RepoName:  repoName,
		Retention: repoInfo.Retention,
		Schema:    repoInfo.Schema,
	}); err != nil {
		return err
	}
	ex.Spec["doc"] = docs
	return c.UpdateExport(&UpdateExportInput{
		RepoName:   repoName,
		ExportName: ex.Name,
		Spec:       ex.Spec,
	})
}

func (c *Pipeline) UpdateRepoWithKodo(input *UpdateRepoInput, ex ExportDesc) error {
	fields, ok := ex.Spec["fields"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("export KODO spec doc assert error %v is not map[string]interface{}", ex.Spec["fields"])
	}
	for _, v := range input.Schema {
		fields[v.Key] = v.Key
	}
	ex.Spec["fields"] = fields
	return c.UpdateExport(&UpdateExportInput{
		RepoName:   input.RepoName,
		ExportName: ex.Name,
		Spec:       ex.Spec,
	})
}

func (c *Pipeline) UpdateRepo(input *UpdateRepoInput) (err error) {
	err = c.getSchemaSorted(input)
	if err != nil {
		return
	}
	err = c.updateRepo(input)
	if err != nil {
		return
	}
	if input.ExportType == "" {
		return nil
	}
	exports, err := c.ListExports(&ListExportsInput{
		RepoName: input.RepoName,
	})
	if err != nil {
		return
	}
	for _, ex := range exports.Exports {
		if input.ExportType == "all" || ex.Type == input.ExportType {
			switch ex.Type {
			case ExportTypeTSDB:
				err = c.UpdateRepoWithTSDB(input, ex)
				if err != nil {
					return
				}
			case ExportTypeLogDB:
				err = c.UpdateRepoWithLogDB(input, ex)
				if err != nil {
					return
				}
			case ExportTypeKODO:
				err = c.UpdateRepoWithKodo(input, ex)
				if err != nil {
					return
				}
			default:
			}
		}
	}
	return nil
}

func (c *Pipeline) updateRepo(input *UpdateRepoInput) (err error) {
	op := c.newOperation(base.OpUpdateRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) GetRepo(input *GetRepoInput) (output *GetRepoOutput, err error) {
	op := c.newOperation(base.OpGetRepo, input.RepoName)

	output = &GetRepoOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) ListRepos(input *ListReposInput) (output *ListReposOutput, err error) {
	op := c.newOperation(base.OpListRepos)

	output = &ListReposOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) DeleteRepo(input *DeleteRepoInput) (err error) {
	op := c.newOperation(base.OpDeleteRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) PostData(input *PostDataInput) (err error) {
	op := c.newOperation(base.OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetBufferBody(input.Points.Buffer())
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeText)
	req.SetFlowLimiter(c.flowLimit)
	req.SetReqLimiter(c.reqLimit)
	return req.Send()
}

type pointContext struct {
	datas  []Data
	inputs *PostDataFromBytesInput
}

func (c *Pipeline) unpack(input *SchemaFreeInput) (packages []pointContext, err error) {
	packages = []pointContext{}
	var buf bytes.Buffer
	var start = 0
	for i, d := range input.Datas {
		point, err := c.generatePoint(input.RepoName, d, !input.NoUpdate)
		if err != nil {
			return nil, err
		}
		pointString := point.ToString()
		// 当buf中有数据，并且加入该条数据后就超过了最大的限制，则提交这个input
		if start < i && buf.Len() > 0 && buf.Len()+len(pointString) >= PandoraMaxBatchSize {
			packages = append(packages, pointContext{
				datas: input.Datas[start:i],
				inputs: &PostDataFromBytesInput{
					RepoName: input.RepoName,
					Buffer:   buf.Bytes(),
				},
			})
			buf.Reset()
			start = i
		}
		buf.WriteString(pointString)
	}
	packages = append(packages, pointContext{
		datas: input.Datas[start:],
		inputs: &PostDataFromBytesInput{
			RepoName: input.RepoName,
			Buffer:   buf.Bytes(),
		},
	})
	return
}

func convertDatas(datas Datas) []map[string]interface{} {
	cdatas := make([]map[string]interface{}, 0)
	for _, v := range datas {
		cdatas = append(cdatas, map[string]interface{}(v))
	}
	return cdatas
}

// PostDataSchemaFree 会更新schema，newSchemas不为nil时就表示更新了，error与否不影响
func (c *Pipeline) PostDataSchemaFree(input *SchemaFreeInput) (newSchemas map[string]RepoSchemaEntry, err error) {
	contexts, err := c.unpack(input)
	if err != nil {
		err = reqerr.NewSendError("Cannot send data to pandora, "+err.Error(), convertDatas(input.Datas), reqerr.TypeDefault)
		return
	}

	failDatas := Datas{}
	errType := reqerr.TypeDefault
	var lastErr error
	c.repoSchemaMux.Lock()
	newSchemas = c.repoSchemas[input.RepoName]
	c.repoSchemaMux.Unlock()
	for _, pContext := range contexts {
		err := c.PostDataFromBytes(pContext.inputs)
		if err != nil {
			reqErr, ok := err.(*reqerr.RequestError)
			if ok {
				switch reqErr.ErrorType {
				case reqerr.InvalidDataSchemaError, reqerr.EntityTooLargeError:
					errType = reqerr.TypeBinaryUnpack
				}
			}
			failDatas = append(failDatas, pContext.datas...)
			lastErr = err
		}
	}
	if len(failDatas) > 0 {
		err = reqerr.NewSendError("Cannot send data to pandora, "+lastErr.Error(), convertDatas(failDatas), errType)
	}
	return
}

func (c *Pipeline) PostDataFromFile(input *PostDataFromFileInput) (err error) {
	op := c.newOperation(base.OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	file, err := os.Open(input.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	stfile, err := file.Stat()
	if err != nil {
		return
	}
	req.SetBodyLength(stfile.Size())
	req.SetReaderBody(file)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeText)
	req.SetFlowLimiter(c.flowLimit)
	req.SetReqLimiter(c.reqLimit)
	return req.Send()
}

func (c *Pipeline) PostDataFromReader(input *PostDataFromReaderInput) (err error) {
	op := c.newOperation(base.OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetReaderBody(input.Reader)
	req.SetBodyLength(input.BodyLength)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeText)
	req.SetFlowLimiter(c.flowLimit)
	req.SetReqLimiter(c.reqLimit)
	return req.Send()
}

func (c *Pipeline) PostDataFromBytes(input *PostDataFromBytesInput) (err error) {
	op := c.newOperation(base.OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetBufferBody(input.Buffer)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeText)
	req.SetFlowLimiter(c.flowLimit)
	req.SetReqLimiter(c.reqLimit)
	return req.Send()
}

func (c *Pipeline) UploadPlugin(input *UploadPluginInput) (err error) {
	op := c.newOperation(base.OpUploadPlugin, input.PluginName)

	req := c.newRequest(op, input.Token, nil)
	req.EnableContentMD5d()
	req.SetBufferBody(input.Buffer.Bytes())
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJar)
	return req.Send()
}

func (c *Pipeline) UploadPluginFromFile(input *UploadPluginFromFileInput) (err error) {
	op := c.newOperation(base.OpUploadPlugin, input.PluginName)

	req := c.newRequest(op, input.Token, nil)
	req.EnableContentMD5d()

	file, err := os.Open(input.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	req.SetReaderBody(file)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJar)
	return req.Send()
}

func (c *Pipeline) ListPlugins(input *ListPluginsInput) (output *ListPluginsOutput, err error) {
	op := c.newOperation(base.OpListPlugins)

	output = &ListPluginsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetPlugin(input *GetPluginInput) (output *GetPluginOutput, err error) {
	op := c.newOperation(base.OpGetPlugin, input.PluginName)

	output = &GetPluginOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeletePlugin(input *DeletePluginInput) (err error) {
	op := c.newOperation(base.OpDeletePlugin, input.PluginName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateTransform(input *CreateTransformInput) (err error) {
	op := c.newOperation(base.OpCreateTransform, input.SrcRepoName, input.TransformName, input.DestRepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input.Spec); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateTransform(input *UpdateTransformInput) (err error) {
	op := c.newOperation(base.OpUpdateTransform, input.SrcRepoName, input.TransformName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input.Spec); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListTransforms(input *ListTransformsInput) (output *ListTransformsOutput, err error) {
	op := c.newOperation(base.OpListTransforms, input.RepoName)

	output = &ListTransformsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetTransform(input *GetTransformInput) (output *GetTransformOutput, err error) {
	op := c.newOperation(base.OpGetTransform, input.RepoName, input.TransformName)

	output = &GetTransformOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteTransform(input *DeleteTransformInput) (err error) {
	op := c.newOperation(base.OpDeleteTransform, input.RepoName, input.TransformName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateExport(input *CreateExportInput) (err error) {
	op := c.newOperation(base.OpCreateExport, input.RepoName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateExport(input *UpdateExportInput) (err error) {
	op := c.newOperation(base.OpUpdateExport, input.RepoName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListExports(input *ListExportsInput) (output *ListExportsOutput, err error) {
	op := c.newOperation(base.OpListExports, input.RepoName)

	output = &ListExportsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetExport(input *GetExportInput) (output *GetExportOutput, err error) {
	op := c.newOperation(base.OpGetExport, input.RepoName, input.ExportName)

	output = &GetExportOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteExport(input *DeleteExportInput) (err error) {
	op := c.newOperation(base.OpDeleteExport, input.RepoName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateDatasource(input *CreateDatasourceInput) (err error) {
	op := c.newOperation(base.OpCreateDatasource, input.DatasourceName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListDatasources() (output *ListDatasourcesOutput, err error) {
	op := c.newOperation(base.OpListDatasources)

	output = &ListDatasourcesOutput{}
	req := c.newRequest(op, "", &output)
	return output, req.Send()
}

func (c *Pipeline) GetDatasource(input *GetDatasourceInput) (output *GetDatasourceOutput, err error) {
	op := c.newOperation(base.OpGetDatasource, input.DatasourceName)

	output = &GetDatasourceOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteDatasource(input *DeleteDatasourceInput) (err error) {
	op := c.newOperation(base.OpDeleteDatasource, input.DatasourceName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateJob(input *CreateJobInput) (err error) {
	op := c.newOperation(base.OpCreateJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
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
	op := c.newOperation(base.OpListJobs, query)

	output = &ListJobsOutput{}
	req := c.newRequest(op, "", &output)
	return output, req.Send()
}

func (c *Pipeline) GetJob(input *GetJobInput) (output *GetJobOutput, err error) {
	op := c.newOperation(base.OpGetJob, input.JobName)

	output = &GetJobOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteJob(input *DeleteJobInput) (err error) {
	op := c.newOperation(base.OpDeleteJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) StartJob(input *StartJobInput) (err error) {
	op := c.newOperation(base.OpStartJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) GetJobHistory(input *GetJobHistoryInput) (output *GetJobHistoryOutput, err error) {
	op := c.newOperation(base.OpGetJobHistory, input.JobName)

	output = &GetJobHistoryOutput{}
	req := c.newRequest(op, input.Token, nil)
	return output, req.Send()
}

func (c *Pipeline) StopJob(input *StopJobInput) (err error) {
	op := c.newOperation(base.OpStopJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateJobExport(input *CreateJobExportInput) (err error) {
	op := c.newOperation(base.OpCreateJobExport, input.JobName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListJobExports(input *ListJobExportsInput) (output *ListJobExportsOutput, err error) {
	op := c.newOperation(base.OpListJobExports, input.JobName)

	output = &ListJobExportsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetJobExport(input *GetJobExportInput) (output *GetJobExportOutput, err error) {
	op := c.newOperation(base.OpGetJobExport, input.JobName, input.ExportName)

	output = &GetJobExportOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteJobExport(input *DeleteJobExportInput) (err error) {
	op := c.newOperation(base.OpDeleteJobExport, input.JobName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) RetrieveSchema(input *RetrieveSchemaInput) (output *RetrieveSchemaOutput, err error) {
	op := c.newOperation(base.OpRetrieveSchema)

	output = &RetrieveSchemaOutput{}
	req := c.newRequest(op, input.Token, &output)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return output, req.Send()
}

func (c *Pipeline) MakeToken(desc *base.TokenDesc) (string, error) {
	return base.MakeTokenInternal(c.Config.Ak, c.Config.Sk, desc)
}

func (c *Pipeline) GetDefault(entry RepoSchemaEntry) interface{} {
	return getDefault(entry)
}

func (c *Pipeline) GetUpdateSchemas(repoName string) (schemas map[string]RepoSchemaEntry, err error) {
	repo, err := c.GetRepo(&GetRepoInput{
		RepoName: repoName,
	})

	if err != nil {
		return
	}
	schemas = make(map[string]RepoSchemaEntry)
	for _, sc := range repo.Schema {
		schemas[sc.Key] = sc
	}
	c.repoSchemaMux.Lock()
	c.repoSchemas[repoName] = schemas
	c.repoSchemaMux.Unlock()
	return
}

func (c *Pipeline) GetLogDBAPI() (logdb.LogdbAPI, error) {
	if c.LogDB == nil {
		logdb, err := logdb.New(c.Config)
		if err != nil {
			return nil, err
		}
		c.LogDB = logdb
	}
	return c.LogDB, nil
}

func (c *Pipeline) GetTSDBAPI() (tsdb.TsdbAPI, error) {
	if c.TSDB == nil {
		tsdb, err := tsdb.New(c.Config)
		if err != nil {
			return nil, err
		}
		c.TSDB = tsdb
	}
	return c.TSDB, nil
}

func (c *Pipeline) CreateForLogDB(input *CreateRepoForLogDBInput) error {
	pinput := formPipelineRepoInput(input.RepoName, input.Region, input.Schema)
	err := c.CreateRepo(pinput)
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	linput := convertCreate2LogDB(input)
	logdbapi, err := c.GetLogDBAPI()
	if err != nil {
		return err
	}
	err = logdbapi.CreateRepo(linput)
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}

	return c.CreateExport(c.FormExportInput(input.RepoName, ExportTypeLogDB, c.FormLogDBSpec(input)))
}

func (c *Pipeline) CreateForLogDBDSL(input *CreateRepoForLogDBDSLInput) error {
	schemas, err := toSchema(input.Schema, 0)
	if err != nil {
		return err
	}
	ci := &CreateRepoForLogDBInput{
		RepoName:    input.RepoName,
		LogRepoName: input.LogRepoName,
		Region:      input.Region,
		Schema:      schemas,
		Retention:   input.Retention,
	}
	return c.CreateForLogDB(ci)
}

func (c *Pipeline) CreateForTSDB(input *CreateRepoForTSDBInput) error {
	pinput := formPipelineRepoInput(input.RepoName, input.Region, input.Schema)
	err := c.CreateRepo(pinput)
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	tsdbapi, err := c.GetTSDBAPI()
	if err != nil {
		return err
	}
	if input.TSDBRepoName == "" {
		input.TSDBRepoName = input.RepoName
	}
	err = tsdbapi.CreateRepo(&tsdb.CreateRepoInput{
		RepoName: input.TSDBRepoName,
		Region:   input.Region,
	})
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	if input.SeriesName == "" {
		input.SeriesName = input.RepoName
	}
	err = tsdbapi.CreateSeries(&tsdb.CreateSeriesInput{
		RepoName:   input.TSDBRepoName,
		SeriesName: input.SeriesName,
		Retention:  input.Retention,
	})
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	return c.CreateExport(c.FormExportInput(input.RepoName, ExportTypeTSDB, c.FormTSDBSpec(input)))
}
