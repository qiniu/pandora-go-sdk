package logdb

import (
	"time"

	. "github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	. "github.com/qiniu/pandora-go-sdk/logdb"
)

var (
	cfg               *config.Config
	client            LogdbAPI
	region            = "<Region>"
	endpoint          = "https://logdb.qiniu.com"
	ak                = "<AccessKey>"
	sk                = "<SecretKey>"
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
		logger.Error("new logdb client failed, err: %v", err)
	}

	defaultRepoSchema = []RepoSchemaEntry{
		RepoSchemaEntry{
			Key:       "f1",
			ValueType: "string",
		},
		RepoSchemaEntry{
			Key:       "f2",
			ValueType: "float",
		},
		RepoSchemaEntry{
			Key:       "f3",
			ValueType: "date",
		},
	}
}

func Sample_Repo() {
	repoName := "repo"
	createInput := &CreateRepoInput{
		RepoName:  repoName,
		Region:    region,
		Schema:    defaultRepoSchema,
		Retention: "2d",
	}

	err := client.CreateRepo(createInput)
	if err != nil {
		logger.Error(err)
		return
	}

	getOutput, err := client.GetRepo(&GetRepoInput{RepoName: repoName})
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(getOutput)

	updateInput := &UpdateRepoInput{
		RepoName:  "repo",
		Schema:    defaultRepoSchema,
		Region:    region,
		Retention: "1d",
	}

	err = client.UpdateRepo(updateInput)
	if err != nil {
		logger.Error(err)
		return
	}

	getOutput, err = client.GetRepo(&GetRepoInput{RepoName: repoName})
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(getOutput)

	listOutput, err := client.ListRepos(&ListReposInput{})
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(listOutput)

	err = client.DeleteRepo(&DeleteRepoInput{RepoName: repoName})
	if err != nil {
		logger.Error(err)
		return
	}
}

func Sample_SendAndQueryLog() {
	repoName := "repo_send_log"
	createInput := &CreateRepoInput{
		RepoName:  repoName,
		Region:    region,
		Schema:    defaultRepoSchema,
		Retention: "2d",
	}
	err := client.CreateRepo(createInput)
	if err != nil {
		logger.Error(err)
		return
	}

	startTime := time.Now().Unix() * 1000
	sendLogInput := &SendLogInput{
		RepoName:       repoName,
		OmitInvalidLog: false,
		Logs: Logs{
			Log{
				"f1": "v11",
				"f2": 1.0,
				"f3": time.Now().UTC().Format(time.RFC3339),
			},
		},
	}
	sendOutput, err := client.SendLog(sendLogInput)
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(sendOutput)
	endTime := time.Now().Unix() * 1000

	histogramInput := &QueryHistogramLogInput{
		RepoName: repoName,
		Query:    "",
		From:     startTime,
		To:       endTime,
		Field:    "f3",
	}
	histogramOutput, err := client.QueryHistogramLog(histogramInput)
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(histogramOutput)

	queryInput := &QueryLogInput{
		RepoName: repoName,
		Query:    "f3:[2016-01-01 TO 2036-01-02]", //query字段sdk会自动做url编码，用户不需要关心
		Sort:     "f2:desc",
		From:     0,
		Size:     100,
	}
	queryOutput, err := client.QueryLog(queryInput)
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(queryOutput)

	err = client.DeleteRepo(&DeleteRepoInput{RepoName: repoName})
	if err != nil {
		logger.Error(err)
		return
	}
}

func Sample_SendLogWithToken() {
	repoName := "repo_send_log_with_token"
	createInput := &CreateRepoInput{
		RepoName:  repoName,
		Region:    region,
		Schema:    defaultRepoSchema,
		Retention: "2d",
	}
	err := client.CreateRepo(createInput)
	if err != nil {
		logger.Error(err)
		return
	}

	// client创建一个对repo_send_log_with_token这个repo进行日志发送的token，它的过期时间是当前时间再加10s
	// 也就是说10秒钟之后再用这个token去发送日志会收到401 Unauthorized错误
	td := &TokenDesc{}
	td.Expires = time.Now().Unix() + 10
	td.Method = "POST"
	td.Url = "/v5/repos/repo_send_log_with_token/data"
	td.ContentType = "application/json"

	token, err := client.MakeToken(td)
	if err != nil {
		logger.Error(err)
		return
	}

	cfg2 := NewConfig().WithEndpoint(endpoint)

	client2, err2 := New(cfg2)
	if err2 != nil {
		logger.Error(err)
		return
	}
	sendLogInput := &SendLogInput{
		RepoName:       repoName,
		OmitInvalidLog: false,
		Logs: Logs{
			Log{
				"f1": "v11",
				"f2": 1.0,
				"f3": time.Now().UTC().Format(time.RFC3339),
			},
			Log{
				"f1": "v21",
				"f2": 1.2,
				"f3": time.Now().UTC().Format(time.RFC3339),
			},
		},
		LogdbToken: LogdbToken{
			Token: token,
		},
	}
	// client2在发送日志的时候拿到的sendLogInput里面包含了client1刚刚给签发的token
	sendOutput, err := client2.SendLog(sendLogInput)
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(sendOutput)

	// 15秒之后再用这个token去访问，就会鉴权不通过
	_, err = client2.SendLog(sendLogInput)
	if err == nil {
		logger.Error(err)
		return
	}

	err = client.DeleteRepo(&DeleteRepoInput{RepoName: repoName})
	if err != nil {
		logger.Error(err)
		return
	}
}
