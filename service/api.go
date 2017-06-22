package service

import (
	"github.com/qiniu/pandora-go-sdk/pipeline"
	"github.com/qiniu/pandora-go-sdk/tsdb"
)

func (s *Service) FormExportInput(repoName, exportType string, spec interface{}) *pipeline.CreateExportInput {
	exportName := repoName + "_export2_" + exportType
	return &pipeline.CreateExportInput{
		RepoName:   repoName,
		ExportName: exportName,
		Type:       exportType,
		Spec:       spec,
	}
}

func (s *Service) FormLogDBSpec(input *CreateRepoInput) *pipeline.ExportLogDBSpec {
	doc := make(map[string]interface{})
	for _, v := range input.Schema {
		doc[v.Key] = v.Key
	}
	return &pipeline.ExportLogDBSpec{
		DestRepoName: input.RepoName,
		Doc:          doc,
	}
}

func (s *Service) FormTSDBSpec(input *CreateRepoInput) *pipeline.ExportTsdbSpec {
	tags := make(map[string]string)
	fields := make(map[string]string)
	for _, v := range input.Schema {
		if v.IsTag {
			tags[v.Key] = v.Key
		} else {
			fields[v.Key] = v.Key
		}
	}
	return &pipeline.ExportTsdbSpec{
		DestRepoName: input.RepoName,
		SeriesName:   input.TSDBseriesName,
		Tags:         tags,
		Fields:       fields,
	}
}

func (s *Service) CreateForLogDB(input *CreateRepoInput) error {
	pinput := convertCreate2Pipeline(input)
	err := s.Pipeline.CreateRepo(pinput)
	if err != nil && !IsExistRepoError(err) {
		return err
	}
	linput := convertCreate2LogDB(input)
	err = s.LogDB.CreateRepo(linput)
	if err != nil && !IsExistRepoError(err) {
		return err
	}

	return s.Pipeline.CreateExport(s.FormExportInput(input.RepoName, pipeline.ExportTypeLogDB, s.FormLogDBSpec(input)))
}

func (s *Service) CreateForTSDB(input *CreateRepoInput) error {
	pinput := convertCreate2Pipeline(input)
	err := s.Pipeline.CreateRepo(pinput)
	if err != nil && !IsExistRepoError(err) {
		return err
	}
	err = s.TSDB.CreateRepo(&tsdb.CreateRepoInput{
		RepoName: input.RepoName,
		Region:   input.Region,
	})
	if err != nil && !IsExistRepoError(err) {
		return err
	}
	err = s.TSDB.CreateSeries(&tsdb.CreateSeriesInput{
		RepoName:   input.RepoName,
		SeriesName: input.TSDBseriesName,
		Retention:  input.Retention,
	})
	if err != nil && !IsExistRepoError(err) {
		return err
	}
	return s.Pipeline.CreateExport(s.FormExportInput(input.RepoName, pipeline.ExportTypeTSDB, s.FormTSDBSpec(input)))
}
