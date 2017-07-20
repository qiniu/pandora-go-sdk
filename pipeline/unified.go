package pipeline

import (
	"strings"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	"github.com/qiniu/pandora-go-sdk/logdb"
	"github.com/qiniu/pandora-go-sdk/tsdb"
)

func (c *Pipeline) FormExportInput(repoName, exportType string, spec interface{}) *CreateExportInput {
	exportName := base.FormExportName(repoName, exportType)
	return &CreateExportInput{
		RepoName:   repoName,
		ExportName: exportName,
		Type:       exportType,
		Spec:       spec,
		Whence:     "oldest",
	}
}

func (c *Pipeline) FormLogDBSpec(RepoName string, Schema []RepoSchemaEntry) *ExportLogDBSpec {
	doc := make(map[string]interface{})
	for _, v := range Schema {
		doc[v.Key] = "#" + v.Key
	}
	return &ExportLogDBSpec{
		DestRepoName: RepoName,
		Doc:          doc,
	}
}

func (c *Pipeline) FormTSDBSpec(TSDBRepoName, seriesName string, rtags []string, Schema []RepoSchemaEntry) *ExportTsdbSpec {
	tags := make(map[string]string)
	fields := make(map[string]string)
	for _, v := range Schema {
		if IsTag(v.Key, rtags) {
			tags[v.Key] = "#" + v.Key
		} else {
			fields[v.Key] = "#" + v.Key
		}
	}
	return &ExportTsdbSpec{
		DestRepoName: TSDBRepoName,
		SeriesName:   seriesName,
		Tags:         tags,
		Fields:       fields,
	}
}

func formPipelineRepoInput(repoName, region string, schemas []RepoSchemaEntry) *CreateRepoInput {
	return &CreateRepoInput{
		Region:   region,
		RepoName: repoName,
		Schema:   schemas,
	}
}

func convertCreate2LogDB(input *CreateRepoForLogDBInput) *logdb.CreateRepoInput {
	if input.LogRepoName == "" {
		input.LogRepoName = input.RepoName
	}
	return &logdb.CreateRepoInput{
		Region:    input.Region,
		RepoName:  input.LogRepoName,
		Schema:    convertSchema2LogDB(input.Schema),
		Retention: input.Retention,
	}
}

func convertSchema2LogDB(scs []RepoSchemaEntry) (ret []logdb.RepoSchemaEntry) {
	ret = make([]logdb.RepoSchemaEntry, 0)
	for _, v := range scs {
		rp := logdb.RepoSchemaEntry{
			Key:       v.Key,
			ValueType: v.ValueType,
		}
		if v.ValueType == PandoraTypeMap {
			rp.Schemas = convertSchema2LogDB(v.Schema)
			rp.ValueType = logdb.TypeObject
		}
		if v.ValueType == PandoraTypeArray {
			rp.ValueType = v.ElemType
		}
		if v.ValueType == PandoraTypeString {
			rp.Analyzer = logdb.KeyWordAnalyzer
		}
		ret = append(ret, rp)
	}
	return ret
}

func (c *Pipeline) AutoExportToTSDB(input *AutoExportToTSDBInput) error {
	if input.TSDBRepoName == "" {
		input.TSDBRepoName = input.RepoName
	}
	if input.SeriesName == "" {
		input.SeriesName = input.RepoName
	}
	if input.Retention == "" {
		input.Retention = "7d"
	}
	repoInfo, err := c.GetRepo(&GetRepoInput{
		RepoName: input.RepoName,
	})
	if err != nil {
		return err
	}
	tsdbapi, err := c.GetTSDBAPI()
	if err != nil {
		return err
	}

	if reqerr.IsNoSuchResourceError(err) {
		err = tsdbapi.CreateRepo(&tsdb.CreateRepoInput{
			RepoName: input.TSDBRepoName,
			Region:   repoInfo.Region,
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
	} else if err != nil {
		return err
	}
	_, err = c.GetExport(&GetExportInput{
		RepoName:   input.RepoName,
		ExportName: base.FormExportName(input.RepoName, ExportTypeTSDB),
	})
	if reqerr.IsNoSuchResourceError(err) {
		return c.CreateExport(c.FormExportInput(input.RepoName, ExportTypeTSDB, c.FormTSDBSpec(input.TSDBRepoName, input.SeriesName, input.Tags, repoInfo.Schema)))
	}
	return nil
}

// 这个api在logkit启动的时候调用一次
func (c *Pipeline) AutoExportToLogDB(input *AutoExportToLogDBInput) error {
	if input.LogRepoName == "" {
		input.LogRepoName = input.RepoName
	}
	input.LogRepoName = strings.ToLower(input.LogRepoName)
	if input.Retention == "" {
		input.Retention = "3d"
	}
	repoInfo, err := c.GetRepo(&GetRepoInput{
		RepoName: input.RepoName,
	})
	if err != nil {
		return err
	}
	logdbapi, err := c.GetLogDBAPI()
	if err != nil {
		return err
	}
	logdbschemas := convertSchema2LogDB(repoInfo.Schema)
	_, err = logdbapi.GetRepo(&logdb.GetRepoInput{
		RepoName: input.LogRepoName,
	})
	if reqerr.IsNoSuchResourceError(err) {
		err = logdbapi.CreateRepo(&logdb.CreateRepoInput{
			RepoName:  input.LogRepoName,
			Region:    repoInfo.Region,
			Retention: input.Retention,
			Schema:    logdbschemas,
		})
		if err != nil && !reqerr.IsExistError(err) {
			return err
		}
	} else if err != nil {
		return err
	}
	_, err = c.GetExport(&GetExportInput{
		RepoName:   input.RepoName,
		ExportName: base.FormExportName(input.RepoName, ExportTypeLogDB),
	})
	if reqerr.IsNoSuchResourceError(err) {
		return c.CreateExport(c.FormExportInput(input.RepoName, ExportTypeLogDB, c.FormLogDBSpec(input.LogRepoName, repoInfo.Schema)))
	}
	return err
}
