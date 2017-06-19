package logdb

import (
	"os"
	"testing"
	"time"

	. "github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	. "github.com/qiniu/pandora-go-sdk/logdb"
	"github.com/stretchr/testify/assert"
)

var (
	cfg               *config.Config
	client            LogdbAPI
	region            = os.Getenv("REGION")
	endpoint          = os.Getenv("LOGDB_HOST")
	ak                = os.Getenv("ACCESS_KEY")
	sk                = os.Getenv("SECRET_KEY")
	logger            Logger
	defaultRepoSchema []RepoSchemaEntry
)

func init() {
	var err error
	logger = NewDefaultLogger()
	cfg = NewConfig().
		WithEndpoint(endpoint).
		WithAccessKeySecretKey(ak, sk).
		WithLogger(logger).
		WithLoggerLevel(LogDebug)

	client, err = New(cfg)
	if err != nil {
		logger.Errorf("new logdb client failed, err: %v", err)
	}

	defaultRepoSchema = []RepoSchemaEntry{
		RepoSchemaEntry{
			Key:       "f1",
			ValueType: "string",
			Analyzer:  "standard",
		},
		RepoSchemaEntry{
			Key:       "f2",
			ValueType: "float",
		},
		RepoSchemaEntry{
			Key:       "f3",
			ValueType: "date",
		},
		RepoSchemaEntry{
			Key:       "f4",
			ValueType: "long",
		},
	}
}

func TestRepo(t *testing.T) {
	repoName := "repo_sdk_test"
	createInput := &CreateRepoInput{
		RepoName:  repoName,
		Region:    region,
		Schema:    defaultRepoSchema,
		Retention: "2d",
	}

	err := client.CreateRepo(createInput)
	if err != nil {
		t.Error(err)
	}

	getOutput, err := client.GetRepo(&GetRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	if getOutput == nil {
		t.Error("repo ret is empty")
	}
	if region != getOutput.Region {
		t.Errorf("unexpected region: %s", region)
	}
	assert.Equal(t, defaultRepoSchema, getOutput.Schema)
	if getOutput.Retention != "2d" {
		t.Errorf("retention should be 2d but %s", getOutput.Retention)
	}

	updateInput := &UpdateRepoInput{
		RepoName:  repoName,
		Schema:    defaultRepoSchema,
		Region:    region,
		Retention: "3d",
	}

	err = client.UpdateRepo(updateInput)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(10 * time.Second)
	getOutput, err = client.GetRepo(&GetRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	if getOutput == nil {
		t.Error("schema ret is empty")
	}
	if "nb" != getOutput.Region {
		t.Error("region should be nb", getOutput.Region)
	}
	assert.Equal(t, defaultRepoSchema, getOutput.Schema)
	if getOutput.Retention != "3d" {
		t.Errorf("retention should be 3d but %s", getOutput.Retention)
	}

	listOutput, err := client.ListRepos(&ListReposInput{})
	if err != nil {
		t.Error(err)
	}
	if listOutput == nil {
		t.Error("repo list should not be empty")
	}
	if listOutput.Repos[0].RepoName != repoName {
		t.Error("repo name is different to origin name")
		t.Error(listOutput.Repos[0].RepoName)
	}

	err = client.DeleteRepo(&DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
}

func TestSendAndQueryLog(t *testing.T) {
	repoName := "repo_send_log"
	createInput := &CreateRepoInput{
		RepoName:  repoName,
		Region:    region,
		Schema:    defaultRepoSchema,
		Retention: "2d",
	}
	err := client.CreateRepo(createInput)
	if err != nil {
		t.Error(err)
	}

	startTime := time.Now().Unix() * 1000
	for i := 0; i < 5; i++ {
		sendLogInput := &SendLogInput{
			RepoName:       repoName,
			OmitInvalidLog: false,
			Logs: Logs{
				Log{
					"f1": "v11",
					"f2": 1.0,
					"f3": time.Now().UTC().Format(time.RFC3339),
					"f4": 1312,
				},
				Log{
					"f1": "v21",
					"f2": 1.2,
					"f3": time.Now().UTC().Format(time.RFC3339),
					"f4": 3082,
				},
				Log{
					"f1": "v31",
					"f2": 0.0,
					"f3": time.Now().UTC().Format(time.RFC3339),
					"f4": 1,
				},
				Log{
					"f1": "v41",
					"f2": 0.3,
					"f3": time.Now().UTC().Format(time.RFC3339),
					"f4": 12345671,
				},
			},
		}
		sendOutput, err := client.SendLog(sendLogInput)
		if err != nil {
			t.Error(err)
		}
		if sendOutput.Success != 4 || sendOutput.Failed != 0 || sendOutput.Total != 4 {
			t.Errorf("send log failed, success: %d, failed: %d, total: %d", sendOutput.Success, sendOutput.Failed, sendOutput.Total)
		}
		time.Sleep(10 * time.Second)
	}
	endTime := time.Now().Unix() * 1000
	time.Sleep(2 * time.Minute)

	histogramInput := &QueryHistogramLogInput{
		RepoName: repoName,
		Query:    "",
		From:     startTime,
		To:       endTime,
		Field:    "f3",
	}
	histogramOutput, err := client.QueryHistogramLog(histogramInput)
	if err != nil {
		t.Error(err)
	}
	if histogramOutput.Total != 20 {
		t.Errorf("log count should be 20, but %d", histogramOutput.Total)
	}
	if histogramOutput.PartialSuccess != false {
		t.Error("query partialSuccess should be false, but true")
	}
	if len(histogramOutput.Buckets) < 5 || len(histogramOutput.Buckets) > 20 {
		t.Errorf("histogram count should ge 5 and le 20, but %d", len(histogramOutput.Buckets))
	}

	queryInput := &QueryLogInput{
		RepoName: repoName,
		Query:    "f3:[2016-01-01 TO 2036-01-02]",
		Sort:     "f2:desc",
		From:     0,
		Size:     100,
	}
	queryOut, err := client.QueryLog(queryInput)
	if err != nil {
		t.Error(err)
	}
	if queryOut.Total != 20 {
		t.Errorf("log count should be 20, but %d", queryOut.Total)
	}
	if queryOut.PartialSuccess != false {
		t.Error("query partialSuccess should be false, but true")
	}
	if len(queryOut.Data) != 20 {
		t.Errorf("log count should be 20, but %d", len(queryOut.Data))
	}
	err = client.DeleteRepo(&DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}

}

func TestSendLogWithToken(t *testing.T) {
	repoName := "repo_send_log_with_token"
	createInput := &CreateRepoInput{
		RepoName:  repoName,
		Region:    region,
		Schema:    defaultRepoSchema,
		Retention: "2d",
	}
	err := client.CreateRepo(createInput)
	if err != nil {
		t.Error(err)
	}

	td := &TokenDesc{}
	td.Expires = time.Now().Unix() + 10
	td.Method = "POST"
	td.Url = "/v5/repos/repo_send_log_with_token/data"
	td.ContentType = "application/json"

	token, err := client.MakeToken(td)
	if err != nil {
		t.Error(err)
	}

	cfg2 := NewConfig().WithEndpoint(endpoint)

	client2, err2 := New(cfg2)
	if err2 != nil {
		logger.Error("new logdb client failed, err: %v", err2)
	}
	sendLogInput := &SendLogInput{
		RepoName:       repoName,
		OmitInvalidLog: false,
		Logs: Logs{
			Log{
				"f1": "v11",
				"f2": 1.0,
				"f3": time.Now().UTC().Format(time.RFC3339),
				"f4": 12,
			},
			Log{
				"f1": "v21",
				"f2": 1.2,
				"f3": time.Now().UTC().Format(time.RFC3339),
				"f4": 2,
			},
		},
		LogdbToken: LogdbToken{
			Token: token,
		},
	}
	_, err = client.SendLog(sendLogInput)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(15 * time.Second)

	_, err = client2.SendLog(sendLogInput)
	if err == nil {
		t.Errorf("expired token: %s, expires: %d, now: %d", token, td.Expires, time.Now().Unix())
	}
	v, ok := err.(*reqerr.RequestError)
	if !ok {
		t.Errorf("cast err to UnauthorizedError fail, err: %v", err)
	}

	if v.ErrorType != reqerr.UnauthorizedError {
		t.Errorf("got errorType: %d, expected errorType: %d", v.ErrorType, reqerr.UnauthorizedError)
	}

	if v.StatusCode != 401 {
		t.Errorf("expires token, expires: %d, now: %d", td.Expires, time.Now().Unix())
	}

	err = client.DeleteRepo(&DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
}

func TestQueryLogWithHighlight(t *testing.T) {
	repoName := "test_sdk_repo_send_log_with_highlight"
	createInput := &CreateRepoInput{
		RepoName:  repoName,
		Region:    region,
		Schema:    defaultRepoSchema,
		Retention: "2d",
	}
	err := client.CreateRepo(createInput)
	if err != nil {
		t.Error(err)
	}

	sendLogInput := &SendLogInput{
		RepoName:       repoName,
		OmitInvalidLog: false,
		Logs: Logs{
			Log{
				"f1": "v11",
				"f2": 1.0,
				"f3": time.Now().UTC().Format(time.RFC3339),
				"f4": 1312,
			},
		},
	}
	sendOutput, err := client.SendLog(sendLogInput)
	if err != nil {
		t.Error(err)
	}
	if sendOutput.Success != 1 || sendOutput.Failed != 0 || sendOutput.Total != 1 {
		t.Errorf("send log failed, success: %d, failed: %d, total: %d", sendOutput.Success, sendOutput.Failed, sendOutput.Total)
	}

	time.Sleep(1 * time.Second)

	queryInput := &QueryLogInput{
		RepoName: repoName,
		Query:    "f1:v11",
		Sort:     "f2:desc",
		From:     0,
		Size:     100,
		Highlight: &Highlight{
			PreTags:  []string{"<em>"},
			PostTags: []string{"</em>"},
			Fields: map[string]interface{}{
				"f1": map[string]string{},
			},
			RequireFieldMatch: false,
			FragmentSize:      1,
		},
	}
	time.Sleep(2 * time.Minute)
	queryOut, err := client.QueryLog(queryInput)
	if err != nil {
		t.Error(err)
	}
	if queryOut.Total != 1 {
		t.Errorf("log count should be 1, but %d", queryOut.Total)
	}
	if queryOut.PartialSuccess != false {
		t.Error("query partialSuccess should be false, but true")
	}
	if len(queryOut.Data) != 1 {
		t.Errorf("log count should be 1, but %d", len(queryOut.Data))
	}
	if queryOut.Data[0]["highlight"] == "" {
		t.Errorf("result don't contain highlight")
	}

	err = client.DeleteRepo(&DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
}
