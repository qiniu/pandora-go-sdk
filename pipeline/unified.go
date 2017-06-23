package pipeline

import "github.com/qiniu/pandora-go-sdk/logdb"

func (c *Pipeline) FormExportInput(repoName, exportType string, spec interface{}) *CreateExportInput {
	exportName := repoName + "_export2_" + exportType
	return &CreateExportInput{
		RepoName:   repoName,
		ExportName: exportName,
		Type:       exportType,
		Spec:       spec,
	}
}

func (c *Pipeline) FormLogDBSpec(input *CreateRepoForLogInput) *ExportLogDBSpec {
	doc := make(map[string]interface{})
	for _, v := range input.Schema {
		doc[v.Key] = v.Key
	}
	return &ExportLogDBSpec{
		DestRepoName: input.RepoName,
		Doc:          doc,
	}
}

func (c *Pipeline) FormTSDBSpec(input *CreateRepoForTSInput) *ExportTsdbSpec {
	tags := make(map[string]string)
	fields := make(map[string]string)
	for _, v := range input.Schema {
		if input.IsTag(v.Key) {
			tags[v.Key] = v.Key
		} else {
			fields[v.Key] = v.Key
		}
	}
	return &ExportTsdbSpec{
		DestRepoName: input.TSDBRepoName,
		SeriesName:   input.SeriesName,
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

func convertCreate2LogDB(input *CreateRepoForLogInput) *logdb.CreateRepoInput {
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
