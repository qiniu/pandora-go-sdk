package service

import (
	"github.com/qiniu/pandora-go-sdk/logdb"
	"github.com/qiniu/pandora-go-sdk/pipeline"
)

func convertCreate2Pipeline(input *CreateRepoInput) *pipeline.CreateRepoInput {
	return &pipeline.CreateRepoInput{
		Region:   input.Region,
		RepoName: input.RepoName,
		Schema:   convertSchema2Pipeline(input.Schema),
	}
}

func convertSchema2Pipeline(scs []RepoSchemaEntry) (ret []pipeline.RepoSchemaEntry) {
	ret = make([]pipeline.RepoSchemaEntry, 0)
	for _, v := range scs {
		rp := pipeline.RepoSchemaEntry{
			Key:       v.Key,
			ValueType: v.ValueType,
		}
		if v.ValueType == pipeline.PandoraTypeMap {
			rp.Schema = convertSchema2Pipeline(v.Schemas)
		}
		if v.ValueType == pipeline.PandoraTypeArray {
			rp.ElemType = v.ElementType
		}
		ret = append(ret, rp)
	}
	return ret
}

func convertCreate2LogDB(input *CreateRepoInput) *logdb.CreateRepoInput {
	return &logdb.CreateRepoInput{
		Region:    input.Region,
		RepoName:  input.RepoName,
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
		if v.ValueType == pipeline.PandoraTypeMap {
			rp.Schemas = convertSchema2LogDB(v.Schemas)
		}
		if v.ValueType == pipeline.PandoraTypeArray {
			rp.ValueType = v.ElementType
		}
		if v.ValueType == pipeline.PandoraTypeString {
			rp.Analyzer = logdb.KeyWordAnalyzer
		}
		ret = append(ret, rp)
	}
	return ret
}
