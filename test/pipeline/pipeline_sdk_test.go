package pipeline

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"strings"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/base/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	"github.com/qiniu/pandora-go-sdk/base/request"
	"github.com/qiniu/pandora-go-sdk/logdb"
	"github.com/qiniu/pandora-go-sdk/pipeline"
	"github.com/qiniu/pandora-go-sdk/tsdb"
	"github.com/stretchr/testify/assert"
)

var (
	cfg      *config.Config
	client   pipeline.PipelineAPI
	logdbapi logdb.LogdbAPI
	tsdbapi  tsdb.TsdbAPI
	region   = os.Getenv("REGION")
	endpoint = os.Getenv("PIPELINE_HOST")
	ak       = os.Getenv("ACCESS_KEY")
	sk       = os.Getenv("SECRET_KEY")
	//endpoint          = os.Getenv("DEV_PIPELINE_HOST")
	//ak                = os.Getenv("DEV_ACCESS_KEY")
	//sk                = os.Getenv("DEV_SECRET_KEY")
	logger            base.Logger
	defaultRepoSchema []pipeline.RepoSchemaEntry
	defaultContainer  *pipeline.Container
	defaultScheduler  *pipeline.JobScheduler
)

func init() {
	var err error
	logger = base.NewDefaultLogger()
	cfg = pipeline.NewConfig().
		WithEndpoint(endpoint).
		WithAccessKeySecretKey(ak, sk).
		WithLogger(logger).
		WithLoggerLevel(base.LogDebug).
		WithLogDBEndpoint(config.DefaultLogDBEndpoint).
		WithTSDBEndpoint(config.DefaultTSDBEndpoint).WithHeaderUserAgent("SDK_TEST")

	tsdbapi, err = tsdb.New(cfg.Clone())
	if err != nil {
		logger.Errorf("new tsdb client failed, err: %v", err)
	}
	client, err = pipeline.New(cfg.Clone())
	if err != nil {
		logger.Errorf("new pipeline client failed, err: %v", err)
	}
	logdbapi, err = logdb.New(cfg.Clone())
	if err != nil {
		logger.Errorf("new logdb client failed, err: %v", err)
	}
	if _, err = logdbapi.GetRepo(&logdb.GetRepoInput{RepoName: "testpandora_go_sdk_init"}); err != nil && !reqerr.IsNoSuchResourceError(err) {
		log.Error(err)
	}
	if _, err = client.GetRepo(&pipeline.GetRepoInput{RepoName: "testpandora_go_sdk_init"}); err != nil && !reqerr.IsNoSuchResourceError(err) {
		log.Error(err)
	}
	if _, err = tsdbapi.GetRepo(&tsdb.GetRepoInput{RepoName: "testpandora_go_sdk_init"}); err != nil && !reqerr.IsNoSuchResourceError(err) {
		log.Error(err)
	}

	defaultRepoSchema = []pipeline.RepoSchemaEntry{
		pipeline.RepoSchemaEntry{
			Key:       "f1",
			ValueType: "string",
			Required:  true,
		},
		pipeline.RepoSchemaEntry{
			Key:       "f2",
			ValueType: "float",
			Required:  true,
		},
	}
	defaultContainer = &pipeline.Container{
		Type:  "1U2G",
		Count: 1,
	}
	defaultScheduler = &pipeline.JobScheduler{
		Type: "loop",
		Spec: &pipeline.JobSchedulerSpec{
			Loop: "10m",
		},
	}
}

func TestUploadUdf(t *testing.T) {
	udfUpload := &pipeline.UploadUdfFromFileInput{
		UdfName:  "testudf",
		FilePath: "./lalalala-0.2-SNAPSHOT.jar",
	}
	err := client.UploadUdfFromFile(udfUpload)
	if err != nil {
		t.Error(err)
	}

	deleteUdf := &pipeline.DeleteUdfInfoInput{
		UdfName: "testudf",
	}
	err = client.DeleteUdf(deleteUdf)
	if err != nil {
		t.Error(err)
	}

	// 多次删除幂等
	err = client.DeleteUdf(deleteUdf)
	if err == nil {
		t.Error(errors.New("多次删除应该返回404"))
	}

	err = client.UploadUdfFromFile(udfUpload)
	if err != nil {
		t.Error(err)
	}

	err = client.PutUdfMeta(&pipeline.PutUdfMetaInput{
		UdfName:     "testudf",
		Description: "这是一个完美的udf",
	})
	if err != nil {
		t.Error(err)
	}

	ret, err := client.ListUdfs(&pipeline.ListUdfsInput{
		PageRequest: pipeline.PageRequest{
			From: 1,
			Size: 1,
			Sort: "uploadTime:asc",
		},
	})
	if ret.Result[0].Description != "这是一个完美的udf" {
		t.Error(errors.New("testudf's description should be 这是一个完美的udf, bug got " + ret.Result[0].Description))
	}

	err = client.PutUdfMeta(&pipeline.PutUdfMetaInput{
		UdfName:     "testudf1",
		Description: "这是一个完美的udf",
	})
	if err == nil {
		t.Error(errors.New("testudf1 should not be exist"))
	}

	udfUpload.UdfName = "testudf1"
	err = client.UploadUdfFromFile(udfUpload)
	if err != nil {
		t.Error(err)
	}

	udfUpload.UdfName = "testudf2"
	err = client.UploadUdfFromFile(udfUpload)
	if err != nil {
		t.Error(err)
	}

	// 按照jarName 顺序排列，分页查询
	ret, err = client.ListUdfs(&pipeline.ListUdfsInput{
		PageRequest: pipeline.PageRequest{
			From: 1,
			Size: 2,
			Sort: "jarName",
		},
	})
	if len(ret.Result) != 2 {
		t.Error(errors.New("按照分页应该只有两条数据"))
	}
	if ret.Result[0].JarName != "testudf" {
		t.Error(errors.New("testudf 应该是第一个数据"))
	}

	// 按照jarName 逆序排列，全部查询
	ret, err = client.ListUdfs(&pipeline.ListUdfsInput{
		PageRequest: pipeline.PageRequest{
			Sort: "jarName:desc",
		},
	})
	if len(ret.Result) != 3 {
		t.Error(errors.New("总共应该有3条数据"))
	}
	if ret.Result[0].JarName != "testudf2" {
		t.Error(errors.New("testudf2 应该是第一个数据"))
	}

	deleteUdf.UdfName = "testudf"
	client.DeleteUdf(deleteUdf)
	deleteUdf.UdfName = "testudf1"
	client.DeleteUdf(deleteUdf)
	deleteUdf.UdfName = "testudf2"
	client.DeleteUdf(deleteUdf)

	_, err = client.ListBuiltinUdfFunctions(&pipeline.ListBuiltinUdfFunctionsInput{
		PageRequest: pipeline.PageRequest{
			From: 1,
			Size: 1,
		},
		Categories: []string{
			"date",
		},
	})
	if err != nil {
		t.Error(err)
	}
}

func TestRegisterUdfFunction(t *testing.T) {
	udfUpload := &pipeline.UploadUdfFromFileInput{
		UdfName:  "testudf",
		FilePath: "./lalalala-0.2-SNAPSHOT.jar",
	}
	err := client.UploadUdfFromFile(udfUpload)
	if err != nil {
		t.Error(err)
	}

	err = client.RegisterUdfFunction(&pipeline.RegisterUdfFunctionInput{
		FuncName:        "a",
		JarName:         "testudf",
		ClassName:       "com.lala.A",
		FuncDeclaration: "..",
		Description:     "...",
	})
	if err == nil {
		t.Error(errors.New("com.lala.A 没有继承 UDF、GenericUDF、GenericUDTF or GenericUDAFResolver"))
	}

	err = client.RegisterUdfFunction(&pipeline.RegisterUdfFunctionInput{
		FuncName:        "b",
		JarName:         "testudf",
		ClassName:       "com.lala.B",
		FuncDeclaration: "..",
		Description:     "...",
	})
	if err == nil {
		t.Error(errors.New("com.lala.B 没有实现 evaluate方法"))
	}

	err = client.RegisterUdfFunction(&pipeline.RegisterUdfFunctionInput{
		FuncName:        "go",
		JarName:         "testudf",
		ClassName:       "com.lala.GOGOGO",
		FuncDeclaration: "..",
		Description:     "...",
	})
	if err == nil {
		t.Error(errors.New("com.lala.GOGOGO 并不存在"))
	}

	err = client.RegisterUdfFunction(&pipeline.RegisterUdfFunctionInput{
		FuncName:        "d",
		JarName:         "testudf",
		ClassName:       "com.lala.D",
		FuncDeclaration: "..",
		Description:     "...",
	})
	if err != nil {
		t.Error(err)
	}

	err = client.RegisterUdfFunction(&pipeline.RegisterUdfFunctionInput{
		FuncName:        "f",
		JarName:         "testudf",
		ClassName:       "com.lala.F",
		FuncDeclaration: "..",
		Description:     "...",
	})
	if err != nil {
		t.Error(err)
	}

	// 按照funcName 逆序排列，分页查询
	ret, err := client.ListUdfFunctions(&pipeline.ListUdfFunctionsInput{
		PageRequest: pipeline.PageRequest{
			From: 1,
			Size: 1,
			Sort: "funcName:desc",
		},
	})
	if len(ret.Result) != 1 {
		t.Error(errors.New("总共应该有1条数据"))
	}
	if ret.Result[0].FuncName != "f" {
		t.Error(errors.New("f 应该是第一个数据 but got " + ret.Result[0].FuncName))
	}

	// 按照funcName 逆序排列，全部查询
	ret, err = client.ListUdfFunctions(&pipeline.ListUdfFunctionsInput{
		PageRequest: pipeline.PageRequest{
			Sort: "funcName:desc",
		},
	})
	if len(ret.Result) != 2 {
		t.Error(errors.New("总共应该有2条数据"))
	}
	if ret.Result[0].FuncName != "f" {
		t.Error(errors.New("f 应该是第一个数据 but got " + ret.Result[0].FuncName))
	}

	err = client.DeRegisterUdfFunction(&pipeline.DeregisterUdfFunctionInput{
		FuncName: "d",
	})
	if err != nil {
		t.Error(err)
	}
	err = client.DeRegisterUdfFunction(&pipeline.DeregisterUdfFunctionInput{
		FuncName: "f",
	})
	if err != nil {
		t.Error(err)
	}

	// 按照funcName 逆序排列，全部查询
	ret, err = client.ListUdfFunctions(&pipeline.ListUdfFunctionsInput{})
	if len(ret.Result) != 0 {
		t.Error(errors.New("总共应该有0条数据"))
	}
}

func writeToFile(path string, content []byte, t *testing.T) {
	file, err := os.Create(path)
	if err != nil {
		t.Error(err)
	}
	if _, err = file.Write(content); err != nil {
		t.Error(err)
	}
	if err = file.Close(); err != nil {
		t.Error(err)
	}
}

func TestRepo(t *testing.T) {
	repoName := "sdk_test_repo"
	createInput := &pipeline.CreateRepoInput{
		RepoName:  repoName,
		Region:    region,
		Schema:    defaultRepoSchema,
		GroupName: "",
	}

	err := client.CreateRepo(createInput)
	if err != nil {
		t.Error(err)
	}

	getOutput, err := client.GetRepo(&pipeline.GetRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	if getOutput == nil {
		t.Error("schema ret is empty")
	}
	if "nb" != getOutput.Region ||
		"" != getOutput.GroupName ||
		!reflect.DeepEqual(defaultRepoSchema, getOutput.Schema) {
		t.Error("spec is different to default spec")
	}

	listOutput, err := client.ListRepos(&pipeline.ListReposInput{})
	if err != nil {
		t.Error(err)
	}
	if listOutput == nil {
		t.Error("repo list should not be empty")
	}

	err = client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
}

func TestRepo_InvalidSpec(t *testing.T) {
	var tests = []struct {
		input *pipeline.CreateRepoInput
	}{
		{
			input: &pipeline.CreateRepoInput{
				RepoName:  "...",
				GroupName: "group",
				Schema: []pipeline.RepoSchemaEntry{
					pipeline.RepoSchemaEntry{
						Key:       "f1",
						ValueType: "string",
						Required:  true,
					},
				},
			},
		},
		{
			input: &pipeline.CreateRepoInput{
				RepoName:  "repo",
				GroupName: "...",
				Schema: []pipeline.RepoSchemaEntry{
					pipeline.RepoSchemaEntry{
						Key:       "f1",
						ValueType: "string",
						Required:  true,
					},
				},
			},
		},
		{
			input: &pipeline.CreateRepoInput{
				RepoName:  "repo",
				GroupName: "",
				Schema:    []pipeline.RepoSchemaEntry{},
			},
		},
		{
			input: &pipeline.CreateRepoInput{
				RepoName:  "repo",
				GroupName: "",
				Schema: []pipeline.RepoSchemaEntry{
					pipeline.RepoSchemaEntry{
						Key:       "...",
						ValueType: "string",
						Required:  true,
					},
				},
			},
		},
		{
			input: &pipeline.CreateRepoInput{
				RepoName:  "repo",
				GroupName: "",
				Schema: []pipeline.RepoSchemaEntry{
					pipeline.RepoSchemaEntry{
						Key:       "f1",
						ValueType: "map",
						Required:  true,
					},
				},
			},
		},
	}

	for i, tt := range tests {
		err := client.CreateRepo(tt.input)
		if err == nil {
			t.Error("index: %s, create repo should failed for invalid input", i)
		}
		if e, ok := err.(*reqerr.RequestError); !ok || e.ErrorType != reqerr.InvalidArgs {
			t.Errorf("index: %d, got err msg: %s", i, err.Error())
		}
	}
}

func TestPlugin(t *testing.T) {
	pluginName := "plugin"
	pluginInput := &pipeline.UploadPluginInput{
		PluginName: pluginName,
		Buffer:     bytes.NewBufferString("plugin content"),
	}
	err := client.UploadPlugin(pluginInput)
	if err != nil {
		t.Error(err)
	}

	getOutput, err := client.GetPlugin(&pipeline.GetPluginInput{PluginName: pluginName})
	if err != nil {
		t.Error(err)
	}
	if getOutput.PluginName != pluginName {
		t.Error("plugin name is different to orign plugin name")
	}

	listOutput, err := client.ListPlugins(&pipeline.ListPluginsInput{})
	if err != nil {
		t.Error(err)
	}
	if len(listOutput.Plugins) != 1 {
		t.Errorf("plugin count should be 1 but %d", len(listOutput.Plugins))
	}
	if listOutput.Plugins[0].PluginName != pluginName {
		t.Errorf("plugin name is different to origin name")
	}

	if err = client.DeletePlugin(&pipeline.DeletePluginInput{PluginName: pluginName}); err != nil {
		t.Error(err)
	}

	path := "/tmp/plugin.jar"
	filePluginInput := &pipeline.UploadPluginFromFileInput{
		PluginName: "plugin",
		FilePath:   path,
	}
	content := []byte("local file plugin content")
	writeToFile(path, content, t)

	if err = client.UploadPluginFromFile(filePluginInput); err != nil {
		t.Error(err)
	}
	if err = client.DeletePlugin(&pipeline.DeletePluginInput{PluginName: pluginName}); err != nil {
		t.Error(err)
	}
}

func TestPostData(t *testing.T) {
	repoName := "repo_post_data"
	createRepoInput := &pipeline.CreateRepoInput{
		RepoName: repoName,
		Schema:   defaultRepoSchema,
		Region:   "nb",
	}
	err := client.CreateRepo(createRepoInput)
	if err != nil {
		t.Error(err)
	}

	postDataInput := &pipeline.PostDataInput{
		RepoName: repoName,
		Points: pipeline.Points{
			pipeline.Point{
				[]pipeline.PointField{
					pipeline.PointField{
						Key:   "f1",
						Value: "12.7",
					},
					pipeline.PointField{
						Key:   "f2",
						Value: 1.0,
					},
				},
			},
		},
	}
	err = client.PostData(postDataInput)
	if err != nil {
		t.Error(err)
	}

	buf := []byte("f1=\"12.7\"\tf2=3.14\nf1=\"dang\"\tf2=1024.0")
	postDataFromBytesInput := &pipeline.PostDataFromBytesInput{
		RepoName: repoName,
		Buffer:   buf,
	}
	err = client.PostDataFromBytes(postDataFromBytesInput)
	if err != nil {
		t.Error(err)
	}

	postDataFromReaderInput := &pipeline.PostDataFromReaderInput{
		RepoName: repoName,
		Reader:   bytes.NewReader(buf),
	}
	err = client.PostDataFromReader(postDataFromReaderInput)
	if err != nil {
		t.Error(err)
	}

	path := "/tmp/postdata"
	writeToFile(path, buf, t)
	postDataFromFileInput := &pipeline.PostDataFromFileInput{
		RepoName: repoName,
		FilePath: path,
	}
	err = client.PostDataFromFile(postDataFromFileInput)
	if err != nil {
		t.Error(err)
	}

	err = client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
}

func TestWithOption(t *testing.T) {
	repoName := "TestWithOption"
	createRepoInput := &pipeline.CreateRepoInput{
		RepoName: repoName,
		Schema:   defaultRepoSchema,
		Region:   "nb",
		Options:  &pipeline.RepoOptions{WithTimestamp: "timestamp", UnescapeLine: true},
	}
	err := client.CreateRepo(createRepoInput)
	if err != nil {
		t.Error(err)
	}

	postDataInput := &pipeline.PostDataInput{
		RepoName: repoName,
		Points: pipeline.Points{
			{
				Fields: []pipeline.PointField{
					{
						Key:   "f1",
						Value: "\\t12.7\\n",
					},
					{
						Key:   "f2",
						Value: 3.14,
					},
				},
			},
			{
				Fields: []pipeline.PointField{
					{
						Key:   "f1",
						Value: "dang\\n\\thehe\nhe11\nx1",
					},
					{
						Key:   "f2",
						Value: 1024.0,
					},
				},
			},
		},
	}
	err = client.PostData(postDataInput)
	if err != nil {
		t.Error(err)
	}

	err = client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}

}

func TestPostDataSchemaLess(t *testing.T) {
	repoName := "repo_post_data_schemaless"
	var err error

	postDataInput := &pipeline.SchemaFreeInput{
		RepoName: repoName,
		Datas: pipeline.Datas{
			pipeline.Data{
				"f1": "12.7",
				"f2": 1.0,
				"f4": 123,
				"f5": true,
			},
		},
	}
	schemas, err := client.PostDataSchemaFree(postDataInput)
	if err != nil {
		t.Error(err)
	}
	log.Println(schemas)
	postDataInput = &pipeline.SchemaFreeInput{
		RepoName: repoName,
		Datas: pipeline.Datas{
			pipeline.Data{
				"f1": "12.7",
				"f3": map[string]interface{}{
					"hello": "123",
				},
			},
		},
	}
	schemas, err = client.PostDataSchemaFree(postDataInput)
	if err != nil {
		t.Error(err)
	}
	log.Println(schemas)
	postDataInput = &pipeline.SchemaFreeInput{
		RepoName: repoName,
		Datas: pipeline.Datas{
			pipeline.Data{
				"f1": "12.7",
				"f3": map[string]interface{}{
					"hello": "123",
					"ketty": 1.23,
				},
			},
		},
	}
	schemas, err = client.PostDataSchemaFree(postDataInput)
	if err != nil {
		t.Error(err)
	}
	log.Println(schemas)
	repo, err := client.GetRepo(&pipeline.GetRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	log.Println(repo.Schema)

	err = client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
}

func TestPostDataRequstLimiter(t *testing.T) {
	repoName := "TestPostDataLimiter"
	createRepoInput := &pipeline.CreateRepoInput{
		RepoName: repoName,
		Schema:   defaultRepoSchema,
		Region:   "nb",
	}
	ncfg := pipeline.NewConfig().
		WithEndpoint(endpoint).
		WithAccessKeySecretKey(ak, sk).
		WithLogger(logger).
		WithLoggerLevel(base.LogDebug).
		WithFlowRateLimit(10)
	nclient, err := pipeline.New(ncfg)
	if err != nil {
		t.Fatal(err)
	}
	defer nclient.Close()

	err = nclient.CreateRepo(createRepoInput)
	if err != nil {
		t.Error(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			postDataInput := &pipeline.PostDataInput{
				RepoName: repoName,
				Points: pipeline.Points{
					pipeline.Point{
						[]pipeline.PointField{
							pipeline.PointField{
								Key:   "f1",
								Value: "1211111221212121212121212121212",
							},
							pipeline.PointField{
								Key:   "f2",
								Value: 1.0,
							},
						},
					},
				},
			}
			for j := 0; j < 100; j++ {
				err = nclient.PostData(postDataInput)
				if err != nil {
					t.Error(err, postDataInput.RepoName)
				}
			}
		}()
	}
	wg.Wait()
	err = nclient.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
}

func TestPostDataGzip(t *testing.T) {
	repoName := "TestPostDataGzip"
	createRepoInput := &pipeline.CreateRepoInput{
		RepoName: repoName,
		Schema:   defaultRepoSchema,
		Region:   "nb",
	}
	ncfg := pipeline.NewConfig().
		WithEndpoint(endpoint).
		WithAccessKeySecretKey(ak, sk).
		WithLogger(logger).
		WithLoggerLevel(base.LogDebug).
		WithFlowRateLimit(1)
	nclient, err := pipeline.New(ncfg)
	if err != nil {
		t.Fatal(err)
	}
	defer nclient.Close()

	err = nclient.CreateRepo(createRepoInput)
	if err != nil {
		t.Error(err)
	}

	var val string
	for i := 0; i < 2048; i++ {
		val += "a"
	}

	postDataInput := &pipeline.PostDataInput{
		RepoName: repoName,
		Points: pipeline.Points{
			pipeline.Point{
				[]pipeline.PointField{
					pipeline.PointField{
						Key:   "f1",
						Value: val,
					},
					pipeline.PointField{
						Key:   "f2",
						Value: 1.0,
					},
				},
			},
		},
	}
	err = nclient.PostData(postDataInput)
	if err == nil {
		t.Error("should have error with flow limit")
	} else {
		fmt.Println(err)
	}

	ncfg1 := pipeline.NewConfig().
		WithEndpoint(endpoint).
		WithAccessKeySecretKey(ak, sk).
		WithLogger(logger).
		WithLoggerLevel(base.LogDebug).
		WithFlowRateLimit(1).WithGzipData(true)
	nclient1, err := pipeline.New(ncfg1)
	if err != nil {
		t.Fatal(err)
	}
	defer nclient1.Close()
	err = nclient1.PostData(postDataInput)
	if err != nil {
		t.Error(err)
	}
	err = nclient.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
}

func TestPostData_WithEscapeCharacters(t *testing.T) {
	repoName := "repo_post_data_with_escape"
	createRepoInput := &pipeline.CreateRepoInput{
		RepoName: repoName,
		Schema:   defaultRepoSchema,
		Region:   "nb",
	}
	err := client.CreateRepo(createRepoInput)
	if err != nil {
		t.Error(err)
	}

	postDataInput := &pipeline.PostDataInput{
		RepoName: repoName,
		Points: pipeline.Points{
			pipeline.Point{
				[]pipeline.PointField{
					pipeline.PointField{
						Key:   "f1",
						Value: "tab: \t xxxxx",
					},
					pipeline.PointField{
						Key:   "f2",
						Value: 1.0,
					},
				},
			},
			pipeline.Point{
				[]pipeline.PointField{
					pipeline.PointField{
						Key:   "f1",
						Value: "newline: \n yyyy",
					},
					pipeline.PointField{
						Key:   "f2",
						Value: 1.0,
					},
				},
			},
			pipeline.Point{
				[]pipeline.PointField{
					pipeline.PointField{
						Key:   "f1",
						Value: "tab: \t and newline: \n zzzz",
					},
					pipeline.PointField{
						Key:   "f2",
						Value: 1.0,
					},
				},
			},
		},
	}
	err = client.PostData(postDataInput)
	if err != nil {
		t.Error(err)
	}

	err = client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
}

func TestTransform(t *testing.T) {
	createRepoInput := &pipeline.CreateRepoInput{
		RepoName: "src_repo",
		Schema:   defaultRepoSchema,
		Region:   "nb",
	}
	err := client.CreateRepo(createRepoInput)
	if err != nil {
		t.Error(err)
	}

	spec := &pipeline.TransformSpec{
		Mode:      "sql",
		Code:      "select * from stream",
		Interval:  "5m",
		Container: defaultContainer,
	}
	createTransInput := &pipeline.CreateTransformInput{
		SrcRepoName:   "src_repo",
		DestRepoName:  "dest_repo",
		TransformName: "transform",
		Spec:          spec,
	}
	err = client.CreateTransform(createTransInput)
	if err != nil {
		t.Error(err)
	}

	updateTransInput := &pipeline.UpdateTransformInput{
		SrcRepoName:   "src_repo",
		TransformName: "transform",
		Spec: &pipeline.TransformSpec{
			Mode: "sql",
			Code: "select f1 from stream",
		},
	}
	err = client.UpdateTransform(updateTransInput)
	if err != nil {
		t.Error(err)
	}

	listTransOutput, err := client.ListTransforms(&pipeline.ListTransformsInput{RepoName: "src_repo"})
	if err != nil {
		t.Error(err)
	}
	if listTransOutput == nil {
		t.Error("listTransOutput should not be empty")
	}

	getTransOutput, err := client.GetTransform(&pipeline.GetTransformInput{RepoName: "src_repo", TransformName: "transform"})
	if err != nil {
		t.Error(err)
	}
	if getTransOutput == nil {
		t.Errorf("getTransInput should be empty")
	}
	if getTransOutput.TransformName != "transform" {
		t.Errorf("transformName should be \"transform\"")
	}
	if getTransOutput.DestRepoName != "dest_repo" {
		t.Errorf("destRepoName should be \"dest_repo\"")
	}
	if getTransOutput.Spec == nil {
		t.Error("spec in getTransOutput should not be empty")
	}
	if getTransOutput.Spec.Mode != "sql" {
		t.Errorf("Mode should be \"sql\"")
	}
	if getTransOutput.Spec.Code != "select * from stream" {
		t.Errorf("Code should be \"select * from stream\"")
	}
	if getTransOutput.Spec.Interval != "5m" {
		t.Errorf("Interval should be \"5m\"")
	}
	if !reflect.DeepEqual(getTransOutput.Spec.Container, defaultContainer) {
		t.Errorf("spec in getTransOutput %v is not equal spec %v", getTransOutput.Spec, spec)
	}
	err = client.DeleteTransform(&pipeline.DeleteTransformInput{RepoName: "src_repo", TransformName: "transform"})
	if err != nil {
		t.Error(err)
	}
	err = client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: "src_repo"})
	if err != nil {
		t.Error(err)
	}
}

func TestExport(t *testing.T) {
	repoName := "PandoraSdkTestExport"
	createRepoInput := &pipeline.CreateRepoInput{
		RepoName: repoName,
		Schema:   defaultRepoSchema,
		Region:   "nb",
	}
	err := client.CreateRepo(createRepoInput)
	if err != nil {
		t.Error(err)
	}
	logdbapi.CreateRepo(&logdb.CreateRepoInput{
		RepoName:  "lg_dest_repo",
		Retention: "1d",
		Region:    "nb",
		Schema:    []logdb.RepoSchemaEntry{logdb.RepoSchemaEntry{Key: "f1", ValueType: "string"}},
	})

	exports := []pipeline.CreateExportInput{
		pipeline.CreateExportInput{
			RepoName:   repoName,
			ExportName: "tsdb_export",
			Spec: &pipeline.ExportTsdbSpec{
				DestRepoName: "tsdb_dest_repo",
				SeriesName:   "series",
				Tags:         map[string]string{"tag1": "#f1"},
				Fields:       map[string]string{"field1": "#f1", "field2": "#f2"},
			},
			Whence: "oldest",
		},
		pipeline.CreateExportInput{
			RepoName:   repoName,
			ExportName: "lg_export_testsdk",
			Spec: &pipeline.ExportLogDBSpec{
				DestRepoName: "lg_dest_repo",
				Doc:          map[string]interface{}{"f1": "f1"},
			},
			Whence: "newest",
		},
		pipeline.CreateExportInput{
			RepoName:   repoName,
			ExportName: "mongo_export",
			Spec: &pipeline.ExportMongoSpec{
				Host:     "10.200.20.23:27017",
				DbName:   "test",
				CollName: "my_coll",
				Mode:     "UPSERT",
				Doc:      map[string]interface{}{"f1": "#f1"},
			},
		},
		pipeline.CreateExportInput{
			RepoName:   repoName,
			ExportName: "kodo_export",
			Spec: &pipeline.ExportKodoSpec{
				Bucket:         "mybucket",
				KeyPrefix:      "export_prefix_",
				Fields:         map[string]string{"field1": "#f1", "field2": "#f2"},
				RotateInterval: 30,
				Email:          "pipeline@qiniu.com",
				AccessKey:      "ak",
				Format:         "text",
				Compress:       true,
				Retention:      3,
			},
		},
		pipeline.CreateExportInput{
			RepoName:   repoName,
			ExportName: "http_export",
			Spec: &pipeline.ExportHttpSpec{
				Host: "http://qiniu.com",
				Uri:  "/resource",
			},
		},
	}

	newspec := &pipeline.ExportLogDBSpec{
		DestRepoName: "lg_dest_repo",
		Doc:          map[string]interface{}{"f1": "#f1", "f2": "#f2"},
	}
	for _, export := range exports {
		err = client.CreateExport(&export)
		if err != nil {
			t.Errorf("export: %s create failed, err: %v", export.ExportName, err)
		}

		getExportInput := &pipeline.GetExportInput{
			RepoName:   export.RepoName,
			ExportName: export.ExportName,
		}
		getExportOutput, err := client.GetExport(getExportInput)
		if err != nil {
			t.Error(err)
		}
		if getExportOutput == nil {
			t.Error("getExportInput should not be nil")
		}
		if getExportOutput.Type != export.Type {
			t.Errorf("type %s is different to expected type %s", getExportOutput.Type, export.Type)
		}
		if getExportOutput.Whence != export.Whence {
			t.Errorf("whence %s is different to expected whence %s", getExportOutput.Whence, export.Whence)
		}
		if export.ExportName == "lg_export_testsdk" {
			err = client.UpdateExport(&pipeline.UpdateExportInput{
				RepoName:   export.RepoName,
				ExportName: export.ExportName,
				Spec:       newspec,
			})
			if err != nil {
				t.Error(err)
			}
			getExportOutput, err = client.GetExport(getExportInput)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(getExportOutput.Spec, newspec) {
				t.Error("Update export error")
			}
		}
	}

	listExportsOutput, err := client.ListExports(&pipeline.ListExportsInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	if listExportsOutput == nil {
		t.Error("listExportsOutput should not be nil")
	}
	if len(listExportsOutput.Exports) != len(exports) {
		t.Errorf("list export count %d should be equal to %d", len(listExportsOutput.Exports), len(exports))
	}

	for _, export := range exports {
		deleteExportInput := &pipeline.DeleteExportInput{
			RepoName:   export.RepoName,
			ExportName: export.ExportName,
		}
		err = client.DeleteExport(deleteExportInput)
		if err != nil {
			t.Error(err)
		}
	}

	err = client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
}

func TestUpdateExport(t *testing.T) {
	exportSchema := []pipeline.RepoSchemaEntry{
		{
			Key:       "f1",
			ValueType: "string",
		},
		{
			Key:       "f2",
			ValueType: "float",
		},
		{
			Key:       "f3",
			ValueType: "map",
			Schema: []pipeline.RepoSchemaEntry{
				{
					Key:       "f1",
					ValueType: "string",
				},
				{
					Key:       "f2",
					ValueType: "float",
				},
			},
		},
	}
	repoName := "pandorasdktestupdateexport"
	createRepoInput := &pipeline.CreateRepoInput{
		RepoName: repoName,
		Schema:   exportSchema,
		Region:   "nb",
	}
	err := client.CreateRepo(createRepoInput)
	if err != nil {
		t.Error(err)
	}
	err = logdbapi.CreateRepo(&logdb.CreateRepoInput{
		RepoName:  repoName,
		Retention: "1d",
		Region:    "nb",
		Schema: []logdb.RepoSchemaEntry{
			{Key: "f1", ValueType: "string"},
			{Key: "f3", ValueType: "object", Schemas: []logdb.RepoSchemaEntry{
				{
					Key:       "f1",
					ValueType: "string",
				},
				{
					Key:       "f2",
					ValueType: "float",
				},
			}},
		},
	})
	if err != nil {
		t.Error(err)
	}
	input := &pipeline.CreateExportInput{
		RepoName:   repoName,
		ExportName: "lg_export",
		Spec: &pipeline.ExportLogDBSpec{
			DestRepoName: repoName,
			Doc:          map[string]interface{}{"f1": "#f1"},
		},
		Whence: "oldest",
	}

	newspec := &pipeline.ExportLogDBSpec{
		DestRepoName: repoName,
		Doc:          map[string]interface{}{"f1": "#f1", "f3": "#f3"},
	}
	err = client.CreateExport(input)
	if err != nil {
		t.Errorf("export: %s create failed, err: %v", input.ExportName, err)
	}

	getExportOutput, err := client.GetExport(&pipeline.GetExportInput{
		RepoName:   input.RepoName,
		ExportName: input.ExportName,
	})
	if err != nil {
		t.Error(err)
	}
	if getExportOutput == nil {
		t.Error("getExportInput should not be nil")
	}
	if getExportOutput.Type != input.Type {
		t.Errorf("type %s is different to expected type %s", getExportOutput.Type, input.Type)
	}
	if getExportOutput.Whence != input.Whence {
		t.Errorf("whence %s is different to expected whence %s", getExportOutput.Whence, input.Whence)
	}
	if input.ExportName == "lg_export" {
		err = client.UpdateExport(&pipeline.UpdateExportInput{
			RepoName:   input.RepoName,
			ExportName: input.ExportName,
			Spec:       newspec,
		})
		if err != nil {
			t.Error(err)
		}
		getExportOutput, err = client.GetExport(&pipeline.GetExportInput{
			RepoName:   input.RepoName,
			ExportName: input.ExportName,
		})
		if err != nil {
			t.Error(err)
		}
		mmp := getExportOutput.Spec["doc"].(map[string]interface{})
		if _, ok := mmp["f3"]; !ok {
			t.Error("spec should be ", newspec, mmp)
		}
	}

	listExportsOutput, err := client.ListExports(&pipeline.ListExportsInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	if listExportsOutput == nil {
		t.Error("listExportsOutput should not be nil")
	}
	if len(listExportsOutput.Exports) != 1 {
		t.Errorf("list export count %d should be equal to %d", len(listExportsOutput.Exports), 1)
	}

	deleteExportInput := &pipeline.DeleteExportInput{
		RepoName:   input.RepoName,
		ExportName: input.ExportName,
	}
	err = client.DeleteExport(deleteExportInput)
	if err != nil {
		t.Error(err)
	}

	err = client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	logdbapi.DeleteRepo(&logdb.DeleteRepoInput{RepoName: repoName})
}

func TestPostDataWithToken(t *testing.T) {
	repoName := "repo_post_data_with_token"
	createInput := &pipeline.CreateRepoInput{
		RepoName: repoName,
		Region:   "nb",
		Schema:   defaultRepoSchema,
	}

	err := client.CreateRepo(createInput)
	if err != nil {
		t.Error(err)
	}

	td := &base.TokenDesc{}
	td.Expires = time.Now().Unix() + 10
	td.Method = "POST"
	td.Url = "/v2/repos/repo_post_data_with_token/data"
	td.ContentType = "text/plain"

	token, err := client.MakeToken(td)
	if err != nil {
		t.Error(err)
	}

	cfg2 := pipeline.NewConfig().WithEndpoint(endpoint)

	client2, err2 := pipeline.New(cfg2)
	if err2 != nil {
		logger.Error("new pipeline client failed, err: %v", err2)
	}
	postDataInput := &pipeline.PostDataInput{
		RepoName: repoName,
		Points: pipeline.Points{
			pipeline.Point{
				[]pipeline.PointField{
					pipeline.PointField{
						Key:   "f1",
						Value: 12.7,
					},
					pipeline.PointField{
						Key:   "f2",
						Value: 1.0,
					},
				},
			},
		},
		PandoraToken: models.PandoraToken{
			Token: token,
		},
	}
	err = client2.PostData(postDataInput)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(15 * time.Second)

	err = client2.PostData(postDataInput)
	if err == nil {
		t.Errorf("expired token: %s, expires: %d, now: %d", token, td.Expires, time.Now().Unix())
	}

	v, ok := err.(*reqerr.RequestError)
	if !ok {
		t.Errorf("cast err to RequestError fail, err: %v", err)
	}

	if v.ErrorType != reqerr.UnauthorizedError {
		t.Errorf("got errorType: %d, expected errorType: %d", v.ErrorType, reqerr.UnauthorizedError)
	}

	if v.StatusCode != 401 {
		t.Errorf("expires token, expires: %d, now: %d", td.Expires, time.Now().Unix())
	}

	err = client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
}

func TestPostDataSchemaFreeWithLOGDB(t *testing.T) {
	repoName := "TestPostDataSchemaFreeWithService"
	var err error

	postDataInput := &pipeline.SchemaFreeInput{
		RepoName: repoName,
		Datas: pipeline.Datas{
			pipeline.Data{
				"f1": "12.7",
				"f2": 1.0,
				"f4": 123,
				"f5": true,
			},
		},
		Option: &pipeline.SchemaFreeOption{
			ToLogDB: true,
			AutoExportToLogDBInput: pipeline.AutoExportToLogDBInput{
				RepoName:  "tologdb",
				Retention: "3d",
			},
		},
	}
	schemas, err := client.PostDataSchemaFree(postDataInput)
	if err != nil {
		t.Error(err)
	}
	logdbrepoinfo, err := logdbapi.GetRepo(&logdb.GetRepoInput{RepoName: postDataInput.Option.AutoExportToLogDBInput.RepoName})
	if err != nil {
		t.Error(err)
	}
	if len(logdbrepoinfo.Schema) != 4 {
		t.Error("logdb repo info error ,schema should be 4 but ", len(logdbrepoinfo.Schema))
	}
	log.Println(logdbrepoinfo.Schema)
	exs, err := client.ListExports(&pipeline.ListExportsInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	if len(exs.Exports) != 1 {
		t.Error("should have 2 exports, but ", len(exs.Exports))
	}
	for _, v := range exs.Exports {
		if v.Type == pipeline.ExportTypeLogDB {
			log.Println(v)
		}
		if v.Type == pipeline.ExportTypeTSDB {

		}
	}
	postDataInput = &pipeline.SchemaFreeInput{
		RepoName: repoName,
		Datas: pipeline.Datas{
			pipeline.Data{
				"f1": "12.7",
				"f3": map[string]interface{}{
					"hello": "123",
				},
			},
		},
		Option: &pipeline.SchemaFreeOption{
			ToLogDB: true,
			AutoExportToLogDBInput: pipeline.AutoExportToLogDBInput{
				RepoName:  "tologdb",
				Retention: "3d",
			},
		},
	}
	schemas, err = client.PostDataSchemaFree(postDataInput)
	if err != nil {
		t.Error(err)
	}
	logdbrepoinfo, err = logdbapi.GetRepo(&logdb.GetRepoInput{RepoName: postDataInput.Option.AutoExportToLogDBInput.RepoName})
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 5, len(logdbrepoinfo.Schema))
	exs, err = client.ListExports(&pipeline.ListExportsInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	if len(exs.Exports) != 1 {
		t.Error("should have 2 exports, but ", len(exs.Exports))
	}
	for _, v := range exs.Exports {
		if v.Type == pipeline.ExportTypeLogDB {
			log.Println(v)
		}
		if v.Type == pipeline.ExportTypeTSDB {

		}
	}
	postDataInput = &pipeline.SchemaFreeInput{
		RepoName: repoName,
		Datas: pipeline.Datas{
			pipeline.Data{
				"f1": "12.7",
				"f3": map[string]interface{}{
					"hello": "123",
					"ketty": 1.23,
				},
			},
		},
		Option: &pipeline.SchemaFreeOption{
			ToLogDB: true,
			AutoExportToLogDBInput: pipeline.AutoExportToLogDBInput{
				RepoName:  "tologdb",
				Retention: "3d",
			},
		},
	}
	schemas, err = client.PostDataSchemaFree(postDataInput)
	if err != nil {
		t.Error(err)
	}
	log.Println(schemas)
	repo, err := client.GetRepo(&pipeline.GetRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	log.Println(repo.Schema)

	listExportsOutput, err := client.ListExports(&pipeline.ListExportsInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	if listExportsOutput == nil {
		t.Error("listExportsOutput should not be nil")
	}
	if len(listExportsOutput.Exports) != 1 {
		t.Errorf("list export count %d should be equal to %d", len(listExportsOutput.Exports), 1)
	}

	for _, export := range listExportsOutput.Exports {
		deleteExportInput := &pipeline.DeleteExportInput{
			RepoName:   repoName,
			ExportName: export.Name,
		}
		err = client.DeleteExport(deleteExportInput)
		if err != nil {
			t.Error(err)
		}
	}
	err = client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Error(err)
	}
	if err = logdbapi.DeleteRepo(&logdb.DeleteRepoInput{RepoName: "tologdb"}); err != nil {
		t.Error(err)
	}
}

func TestQuerySearch(t *testing.T) {
	repoName := "querydag"
	createRepoInput := &pipeline.CreateRepoInput{
		RepoName: repoName,
		Schema:   defaultRepoSchema,
		Region:   "nb",
	}
	defer func() {
		err := client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: repoName})
		if err != nil {
			t.Error(err)
		}
	}()

	err := client.CreateRepo(createRepoInput)
	if err != nil {
		t.Error(err)
	}

	ret, err := client.SearchWorkflow(&pipeline.DagLogSearchInput{
		WorkflowName: "zhp3",
		Region:       "nb",
		Type:         "export",
		Name:         "zhp3_logdbExport1",
		Repo:         "zhp3_flowDataSource1",
		Query:        "*",
		Size:         10,
	})
	assert.NoError(t, err)
	fmt.Println(ret)

}

func TestGetPluginVerify(t *testing.T) {
	out, err := client.VerifyPlugin(&pipeline.VerifyPluginInput{PluginName: "com.package.WordSegmentParserV3"})
	assert.NoError(t, err)
	fmt.Println(out)
}

func TestVariables(t *testing.T) {
	err := client.CreateVariable(&pipeline.CreateVariableInput{
		Name:   "timeVariable",
		Type:   pipeline.VariableTimeType,
		Value:  "$(now)-1d",
		Format: "yyyy-MM-dd HH:mm:ss",
	})
	if err != nil {
		t.Error(err)
	}

	err = client.CreateVariable(&pipeline.CreateVariableInput{
		Name:  "stringVariable",
		Type:  pipeline.VariableStringType,
		Value: "constants",
	})
	if err != nil {
		t.Error(err)
	}

	userVars, err := client.ListUserVariables(&pipeline.ListVariablesInput{})
	if err != nil {
		t.Error(err)
	}
	t.Logf("userVariables is: %v", userVars)

	systemVars, err := client.ListSystemVariables(&pipeline.ListVariablesInput{})
	if err != nil {
		t.Error(err)
	}
	t.Logf("systemVariables is: %v", systemVars)
}

func TestWorkflow(t *testing.T) {
	workflowName := "workflow_test"
	var err error
	defer func() {
		err = client.DeleteExport(&pipeline.DeleteExportInput{
			RepoName:   "my_test_repo",
			ExportName: "my_test_export",
		})
		if err != nil {
			t.Error(err)
		}

		err = client.DeleteTransform(&pipeline.DeleteTransformInput{
			RepoName:      "my_test_repo",
			TransformName: "my_test_transform",
		})
		if err != nil {
			t.Error(err)
		}

		err = client.DeleteRepo(&pipeline.DeleteRepoInput{RepoName: "my_test_repo"})
		if err != nil {
			t.Error(err)
		}

		err = client.DeleteJobExport(&pipeline.DeleteJobExportInput{JobName: "my_test_job", ExportName: "my_test_jobexport"})
		if err != nil {
			t.Error(err)
		}

		err = client.DeleteJob(&pipeline.DeleteJobInput{JobName: "my_test_job"})
		if err != nil {
			t.Error(err)
		}

		err = client.DeleteDatasource(&pipeline.DeleteDatasourceInput{DatasourceName: "my_test_datasource"})
		if err != nil {
			t.Error(err)
		}
		err := client.DeleteWorkflow(&pipeline.DeleteWorkflowInput{WorkflowName: workflowName})
		if err != nil {
			t.Error(err)
		}
	}()
	t.Log(">>>>>>>>>>>>>>>>> begin workflow test >>>>>>>>>>>>>>>>>>>>>>>>")
	createWorkflowInput := &pipeline.CreateWorkflowInput{
		WorkflowName: workflowName,
		Comment:      "myComment",
		Region:       "nb",
	}
	err = client.CreateWorkflow(createWorkflowInput)
	if err != nil {
		t.Error(err)
	}

	repoName := "my_test_repo"
	createRepoInput := &pipeline.CreateRepoInput{
		RepoName: repoName,
		Schema:   defaultRepoSchema,
		Region:   "nb",
		Workflow: workflowName,
	}
	err = client.CreateRepo(createRepoInput)
	if err != nil {
		t.Error(err)
	}

	transName := "my_test_transform"
	dstRepoName := "my_test_repo_dst"
	transformSpec := &pipeline.TransformSpec{
		Mode:      "sql",
		Code:      "select * from stream",
		Interval:  "1m",
		Container: defaultContainer,
	}
	createTransInput := &pipeline.CreateTransformInput{
		SrcRepoName:   repoName,
		DestRepoName:  dstRepoName,
		TransformName: transName,
		Spec:          transformSpec,
	}
	err = client.CreateTransform(createTransInput)
	if err != nil {
		t.Error(err)
	}

	exportHttpSpec := &pipeline.ExportHttpSpec{
		Host:   "http://10.200.20.40:9090/",
		Uri:    "/home/qboxserver/integration_test/test_export_http",
		Format: "json",
	}

	createExportInput := &pipeline.CreateExportInput{
		RepoName:   "my_test_repo",
		ExportName: "my_test_export",
		Type:       "http",
		Spec:       exportHttpSpec,
		Whence:     "newest",
	}
	err = client.CreateExport(createExportInput)
	if err != nil {
		t.Error(err)
	}

	datasourceName := "my_test_datasource"
	keyPrefixesJson := []string{"batch/json"}
	batchDatasourceBucket := "integrationtest"

	kodoSourceSpec := &pipeline.KodoSourceSpec{
		Bucket:      batchDatasourceBucket,
		KeyPrefixes: keyPrefixesJson,
		FileType:    "json",
	}

	schema := []pipeline.RepoSchemaEntry{
		pipeline.RepoSchemaEntry{
			Key:       "fstring",
			ValueType: "string",
			Required:  true,
		},
		pipeline.RepoSchemaEntry{
			Key:       "flong",
			ValueType: "long",
			Required:  true,
		},
	}

	createDatasourceInput := &pipeline.CreateDatasourceInput{
		DatasourceName: datasourceName,
		Spec:           kodoSourceSpec,
		Region:         "nb",
		Type:           "kodo",
		Schema:         schema,
		Workflow:       workflowName,
	}
	err = client.CreateDatasource(createDatasourceInput)
	if err != nil {
		t.Error(err)
	}

	jobName := "my_test_job"
	tableName := "tbl"
	jobSrcs := []pipeline.JobSrc{
		pipeline.JobSrc{
			SrcName:    datasourceName,
			FileFilter: "",
			Type:       "datasource",
			TableName:  tableName,
		},
	}

	computation := pipeline.Computation{
		Code: fmt.Sprintf("select * from %s", tableName),
		Type: "sql",
	}

	createJobInput := &pipeline.CreateJobInput{
		JobName:     jobName,
		Container:   defaultContainer,
		Srcs:        jobSrcs,
		Computation: computation,
		Scheduler:   defaultScheduler,
	}
	err = client.CreateJob(createJobInput)
	if err != nil {
		t.Error(err)
	}

	jobexportName := "my_test_jobexport"
	jobexportSpec := &pipeline.JobExportKodoSpec{
		Bucket:      batchDatasourceBucket,
		KeyPrefix:   "test/1121",
		Format:      "json",
		Retention:   1,
		FileCount:   1,
		SaveMode:    "append",
		PartitionBy: []string{},
	}
	createJobexportInput := &pipeline.CreateJobExportInput{
		JobName:    jobName,
		ExportName: jobexportName,
		Type:       "kodo",
		Spec:       jobexportSpec,
	}
	err = client.CreateJobExport(createJobexportInput)
	if err != nil {
		t.Error(err)
	}

	err = client.StartWorkflow(&pipeline.StartWorkflowInput{WorkflowName: workflowName})
	if err != nil {
		t.Error(err)
	}

	err = pipeline.WaitWorkflowStarted(workflowName, client, logger, models.PandoraToken{})
	if err != nil {
		t.Error(err)
	}

	err = client.StopWorkflow(&pipeline.StopWorkflowInput{WorkflowName: workflowName})
	if err != nil {
		t.Error(err)
	}

	err = pipeline.WaitWorkflowStopped(workflowName, client, logger, models.PandoraToken{})
	if err != nil {
		t.Error(err)
	}

	getDatasourceOutput, err := client.GetDatasource(&pipeline.GetDatasourceInput{
		DatasourceName: "my_test_datasource",
	})
	if err != nil {
		t.Error(err)
	}
	t.Log(getDatasourceOutput)

	getJobOutput, err := client.GetJob(&pipeline.GetJobInput{
		JobName: "my_test_job",
	})
	if err != nil {
		t.Error(err)
	}
	t.Log(getJobOutput)

	getExportOutput, err := client.GetJobExport(&pipeline.GetJobExportInput{
		JobName:    "my_test_job",
		ExportName: "my_test_jobexport",
	})
	if err != nil {
		t.Error(err)
	}
	t.Log(getExportOutput)

	getRepoOutput, err := client.GetRepo(&pipeline.GetRepoInput{
		RepoName: "my_test_repo",
	})
	if err != nil {
		t.Error(err)
	}
	t.Log(getRepoOutput)

	getTransformOut, err := client.GetTransform(&pipeline.GetTransformInput{
		RepoName:      "my_test_repo",
		TransformName: "my_test_transform",
	})
	if err != nil {
		t.Error(err)
	}
	t.Log(getTransformOut)

	getExportOut, err := client.GetExport(&pipeline.GetExportInput{
		RepoName:   "my_test_repo",
		ExportName: "my_test_export",
	})
	if err != nil {
		t.Error(err)
	}
	t.Log(getExportOut)

	assert.NoError(t, err)
}

/*
type SchemaFreeToken struct {
	PipelineCreateRepoToken        PandoraToken
	PipelinePostDataToken          PandoraToken
	PipelineGetRepoToken           PandoraToken
	PipelineUpdateRepoToken        PandoraToken
	PipelineGetWorkflowToken       PandoraToken
	PipelineCreateWorkflowToken    PandoraToken
	PipelineStartWorkflowToken     PandoraToken
	PipelineStopWorkflowToken      PandoraToken
	PipelineGetWorkflowStatusToken PandoraToken
}
*/
func GenerateTokensForSchemaFree(repoName, workflowName, ak, sk string) map[string]models.PandoraToken {

	typeTextPlain := "text/plain"
	typeAppJson := "application/json"
	tokens := make(map[string]models.PandoraToken)
	ops := []struct {
		Op   string
		Arg1 interface{}
		Arg2 interface{}
		Ctp  string
	}{
		{
			Op:   base.OpCreateRepo,
			Arg1: repoName,
			Ctp:  typeAppJson,
		},
		{
			Op:   base.OpPostData,
			Arg1: repoName,
			Ctp:  typeTextPlain,
		},
		{
			Op:   base.OpGetRepo,
			Arg1: repoName,
		},
		{
			Op:   base.OpUpdateRepo,
			Arg1: repoName,
			Ctp:  typeAppJson,
		},
		{
			Op:   base.OpGetWorkflow,
			Arg1: workflowName,
		},
		{
			Op:   base.OpCreateWorkflow,
			Arg1: workflowName,
			Ctp:  typeAppJson,
		},
		{
			Op:   base.OpStartWorkflow,
			Arg1: workflowName,
		},
		{
			Op:   base.OpStopWorkflow,
			Arg1: workflowName,
		},
		{
			Op:   base.OpGetWorkflowStatus,
			Arg1: workflowName,
		},
	}

	c, _ := pipeline.NewDefaultClient(pipeline.NewConfig())
	for _, v := range ops {
		op := c.NewOperation(v.Op, v.Arg1)

		desc := base.TokenDesc{
			Url:     op.Path,
			Expires: time.Now().Add(24 * time.Hour).Unix(),
			Method:  op.Method,
		}
		if v.Ctp != "" {
			desc.ContentType = v.Ctp
		}
		token, err := base.MakeTokenInternal(ak, sk, &desc)
		if err != nil {
			log.Fatalf("%v %v", v.Op, err)
		}
		tokens[v.Op] = models.PandoraToken{Token: token}
	}
	return tokens
}

const (
	pdPipeline = "pipeline"
	pdLogdb    = "logdb"
	pdTSDB     = "tsdb"
)

/*
type AutoExportLogDBTokens struct {
	PipelineCreateRepoToken PandoraToken
	PipelineGetRepoToken    PandoraToken
	CreateLogDBRepoToken    PandoraToken
	UpdateLogDBRepoToken    PandoraToken
	GetLogDBRepoToken       PandoraToken
	CreateExportToken       PandoraToken
	UpdateExportToken       PandoraToken
	GetExportToken          PandoraToken
	ListExportToken         PandoraToken
}
*/

func GenerateTokensForAutoLogdb(pipelineRepoName, logdbRepoName, ak, sk string) map[string]models.PandoraToken {
	typeAppJson := "application/json"
	if logdbRepoName == "" {
		logdbRepoName = pipelineRepoName
	}
	logdbRepoName = strings.ToLower(logdbRepoName)
	tokens := make(map[string]models.PandoraToken)
	ops := []struct {
		Op   string
		Arg1 interface{}
		Arg2 interface{}
		Ctp  string
		pd   string
	}{
		{
			Op:   base.OpCreateRepo,
			Arg1: pipelineRepoName,
			Ctp:  typeAppJson,
			pd:   pdPipeline,
		},
		{
			Op:   base.OpGetRepo,
			Arg1: pipelineRepoName,
			pd:   pdPipeline,
		},
		{
			Op:   base.OpCreateRepo,
			Arg1: logdbRepoName,
			Ctp:  typeAppJson,
			pd:   pdLogdb,
		},
		{
			Op:   base.OpUpdateRepo,
			Arg1: logdbRepoName,
			Ctp:  typeAppJson,
			pd:   pdLogdb,
		},
		{
			Op:   base.OpGetRepo,
			Arg1: logdbRepoName,
			pd:   pdLogdb,
		},
		{
			Op:   base.OpCreateExport,
			Arg1: pipelineRepoName,
			Arg2: base.FormExportName(pipelineRepoName, pipeline.ExportTypeLogDB),
			Ctp:  typeAppJson,
			pd:   pdPipeline,
		},
		{
			Op:   base.OpUpdateExport,
			Arg1: pipelineRepoName,
			Arg2: base.FormExportName(pipelineRepoName, pipeline.ExportTypeLogDB),
			Ctp:  typeAppJson,
			pd:   pdPipeline,
		},
		{
			Op:   base.OpGetExport,
			Arg1: pipelineRepoName,
			Arg2: base.FormExportName(pipelineRepoName, pipeline.ExportTypeLogDB),
			pd:   pdPipeline,
		},
		{
			Op:   base.OpListExports,
			Arg1: pipelineRepoName,
			pd:   pdPipeline,
		},
	}

	pc, _ := pipeline.NewDefaultClient(pipeline.NewConfig())
	lc, _ := logdb.NewClient(logdb.NewConfig())
	for _, v := range ops {
		var desc base.TokenDesc
		switch v.pd {
		case pdLogdb:
			var op *request.Operation
			if v.Arg2 == nil {
				op = lc.NewOperation(v.Op, v.Arg1)
			} else {
				op = lc.NewOperation(v.Op, v.Arg1, v.Arg2)
			}
			desc = base.TokenDesc{
				Url:     op.Path,
				Expires: time.Now().Add(24 * time.Hour).Unix(),
				Method:  op.Method,
			}
		case pdPipeline:
			var op *request.Operation
			if v.Arg2 == nil {
				op = pc.NewOperation(v.Op, v.Arg1)
			} else {
				op = pc.NewOperation(v.Op, v.Arg1, v.Arg2)
			}
			desc = base.TokenDesc{
				Url:     op.Path,
				Expires: time.Now().Add(24 * time.Hour).Unix(),
				Method:  op.Method,
			}
		}
		if v.Ctp != "" {
			desc.ContentType = v.Ctp
		}
		token, err := base.MakeTokenInternal(ak, sk, &desc)
		if err != nil {
			log.Fatalf("%v %v %v", v.Op, desc, err)
		}
		tokens[v.pd+"-"+v.Op] = models.PandoraToken{Token: token}
	}
	return tokens
}

/*
type AutoExportKodoTokens struct {
	PipelineGetRepoToken PandoraToken
	CreateExportToken    PandoraToken
	UpdateExportToken    PandoraToken
	GetExportToken       PandoraToken
	ListExportToken      PandoraToken
}
*/

func GenerateTokensForAutoKodo(pipelineRepoName, bucketname, ak, sk string) map[string]models.PandoraToken {
	typeAppJson := "application/json"
	tokens := make(map[string]models.PandoraToken)
	ops := []struct {
		Op   string
		Arg1 interface{}
		Arg2 interface{}
		Ctp  string
		pd   string
	}{
		{
			Op:   base.OpGetRepo,
			Arg1: pipelineRepoName,
			pd:   pdPipeline,
		},
		{
			Op:   base.OpCreateExport,
			Arg1: pipelineRepoName,
			Arg2: base.FormExportName(pipelineRepoName, pipeline.ExportTypeKODO),
			Ctp:  typeAppJson,
			pd:   pdPipeline,
		},
		{
			Op:   base.OpUpdateExport,
			Arg1: pipelineRepoName,
			Arg2: base.FormExportName(pipelineRepoName, pipeline.ExportTypeKODO),
			Ctp:  typeAppJson,
			pd:   pdPipeline,
		},
		{
			Op:   base.OpGetExport,
			Arg1: pipelineRepoName,
			Arg2: base.FormExportName(pipelineRepoName, pipeline.ExportTypeKODO),
			pd:   pdPipeline,
		},
		{
			Op:   base.OpListExports,
			Arg1: pipelineRepoName,
			pd:   pdPipeline,
		},
	}

	pc, _ := pipeline.NewDefaultClient(pipeline.NewConfig())
	for _, v := range ops {
		var desc base.TokenDesc
		var op *request.Operation
		if v.Arg2 == nil {
			op = pc.NewOperation(v.Op, v.Arg1)
		} else {
			op = pc.NewOperation(v.Op, v.Arg1, v.Arg2)
		}
		desc = base.TokenDesc{
			Url:     op.Path,
			Expires: time.Now().Add(24 * time.Hour).Unix(),
			Method:  op.Method,
		}
		if v.Ctp != "" {
			desc.ContentType = v.Ctp
		}
		token, err := base.MakeTokenInternal(ak, sk, &desc)
		if err != nil {
			log.Fatalf("%v %v", v.Op, err)
		}
		tokens[v.Op] = models.PandoraToken{Token: token}
	}
	return tokens
}

/*
type AutoExportTSDBTokens struct {
	PipelineGetRepoToken   PandoraToken
	CreateTSDBRepoToken    PandoraToken
	CreateTSDBSeriesTokens map[string]PandoraToken
	CreateExportToken      PandoraToken
	UpdateExportToken      PandoraToken
	GetExportToken         PandoraToken
	ListExportToken        PandoraToken
}
*/

func GenerateTokensForAutoTSDB(pipelineRepoName, tsdbRepoName, ak, sk string) map[string]models.PandoraToken {
	typeAppJson := "application/json"
	if tsdbRepoName == "" {
		tsdbRepoName = pipelineRepoName
	}
	tokens := make(map[string]models.PandoraToken)
	ops := []struct {
		Op   string
		Arg1 interface{}
		Arg2 interface{}
		Ctp  string
		pd   string
	}{
		{
			Op:   base.OpGetRepo,
			Arg1: pipelineRepoName,
			pd:   pdPipeline,
		},
		{
			Op:   base.OpCreateRepo,
			Arg1: tsdbRepoName,
			Ctp:  typeAppJson,
			pd:   pdTSDB,
		},
		{
			Op:   base.OpGetRepo,
			Arg1: tsdbRepoName,
			pd:   pdTSDB,
		},
		{
			Op:   base.OpCreateExport,
			Arg1: pipelineRepoName,
			Arg2: base.FormExportName(pipelineRepoName, pipeline.ExportTypeTSDB),
			Ctp:  typeAppJson,
			pd:   pdPipeline,
		},
		{
			Op:   base.OpUpdateExport,
			Arg1: pipelineRepoName,
			Arg2: base.FormExportName(pipelineRepoName, pipeline.ExportTypeTSDB),
			Ctp:  typeAppJson,
			pd:   pdPipeline,
		},
		{
			Op:   base.OpGetExport,
			Arg1: pipelineRepoName,
			Arg2: base.FormExportName(pipelineRepoName, pipeline.ExportTypeTSDB),
			pd:   pdPipeline,
		},
		{
			Op:   base.OpListExports,
			Arg1: pipelineRepoName,
			pd:   pdPipeline,
		},
	}

	pc, _ := pipeline.NewDefaultClient(pipeline.NewConfig())
	tc, _ := tsdb.NewDefaultClient(tsdb.NewConfig())
	for _, v := range ops {
		var desc base.TokenDesc
		switch v.pd {
		case pdTSDB:
			var op *request.Operation
			if v.Arg2 == nil {
				op = tc.NewOperation(v.Op, v.Arg1)
			} else {
				op = tc.NewOperation(v.Op, v.Arg1, v.Arg2)
			}
			desc = base.TokenDesc{
				Url:     op.Path,
				Expires: time.Now().Add(24 * time.Hour).Unix(),
				Method:  op.Method,
			}
		case pdPipeline:
			var op *request.Operation
			if v.Arg2 == nil {
				op = pc.NewOperation(v.Op, v.Arg1)
			} else {
				op = pc.NewOperation(v.Op, v.Arg1, v.Arg2)
			}
			desc = base.TokenDesc{
				Url:     op.Path,
				Expires: time.Now().Add(24 * time.Hour).Unix(),
				Method:  op.Method,
			}
		}
		if v.Ctp != "" {
			desc.ContentType = v.Ctp
		}
		token, err := base.MakeTokenInternal(ak, sk, &desc)
		if err != nil {
			log.Fatalf("%v %v", v.Op, err)
		}
		tokens[v.pd+"-"+v.Op] = models.PandoraToken{Token: token}
	}
	return tokens
}

func GenerateTokensForTSDBSeries(pipelineRepoName, tsdbRepoName, ak, sk string, tsdbserisNames []string) (createSeriesTokens, createExportTokens, updateExportTokens, getExportTokens map[string]models.PandoraToken) {
	typeAppJson := "application/json"
	if tsdbRepoName == "" {
		tsdbRepoName = pipelineRepoName
	}
	createSeriesTokens = make(map[string]models.PandoraToken)
	createExportTokens = make(map[string]models.PandoraToken)
	updateExportTokens = make(map[string]models.PandoraToken)
	getExportTokens = make(map[string]models.PandoraToken)
	tc, _ := tsdb.NewDefaultClient(tsdb.NewConfig())
	pc, _ := pipeline.NewDefaultClient(pipeline.NewConfig())

	for _, v := range tsdbserisNames {
		op := tc.NewOperation(base.OpCreateSeries, tsdbRepoName, v)
		desc := base.TokenDesc{
			Url:         op.Path,
			Expires:     time.Now().Add(24 * time.Hour).Unix(),
			Method:      op.Method,
			ContentType: typeAppJson,
		}
		token, err := base.MakeTokenInternal(ak, sk, &desc)
		if err != nil {
			log.Fatalf("%v %v", base.OpCreateSeries, err)
		}
		createSeriesTokens[v] = models.PandoraToken{Token: token}

		op = pc.NewOperation(base.OpCreateExport, pipelineRepoName, base.FormExportTSDBName(pipelineRepoName, v, pipeline.ExportTypeTSDB))
		desc = base.TokenDesc{
			Url:         op.Path,
			Expires:     time.Now().Add(24 * time.Hour).Unix(),
			Method:      op.Method,
			ContentType: typeAppJson,
		}
		token, err = base.MakeTokenInternal(ak, sk, &desc)
		if err != nil {
			log.Fatalf("%v %v", base.OpCreateExport, err)
		}
		createExportTokens[base.FormExportTSDBName(pipelineRepoName, v, pipeline.ExportTypeTSDB)] = models.PandoraToken{Token: token}

		op = pc.NewOperation(base.OpUpdateExport, pipelineRepoName, base.FormExportTSDBName(pipelineRepoName, v, pipeline.ExportTypeTSDB))
		desc = base.TokenDesc{
			Url:         op.Path,
			Expires:     time.Now().Add(24 * time.Hour).Unix(),
			Method:      op.Method,
			ContentType: typeAppJson,
		}
		token, err = base.MakeTokenInternal(ak, sk, &desc)
		if err != nil {
			log.Fatalf("%v %v", base.OpUpdateExport, err)
		}
		updateExportTokens[base.FormExportTSDBName(pipelineRepoName, v, pipeline.ExportTypeTSDB)] = models.PandoraToken{Token: token}

		op = pc.NewOperation(base.OpGetExport, pipelineRepoName, base.FormExportTSDBName(pipelineRepoName, v, pipeline.ExportTypeTSDB))
		desc = base.TokenDesc{
			Url:         op.Path,
			Expires:     time.Now().Add(24 * time.Hour).Unix(),
			Method:      op.Method,
			ContentType: typeAppJson,
		}
		token, err = base.MakeTokenInternal(ak, sk, &desc)
		if err != nil {
			log.Fatalf("%v %v", base.OpGetExport, err)
		}
		getExportTokens[base.FormExportTSDBName(pipelineRepoName, v, pipeline.ExportTypeTSDB)] = models.PandoraToken{Token: token}
	}
	return
}

func TestPostDataSchemaFreeWithToken(t *testing.T) {
	pipelineRepoName := "TestPostDataSchemaFreeWithTokennn13"
	worklow := "TestPostDataSchemaFreeWithWorkflow1"
	logdbRepoName := "TestPostDataSchemaFreeWithTokenLl2"
	tsdbRepoName := "TestPostDataSchemaFreeWithTokenTt2"
	bucketName := "defaut"

	schematokens := GenerateTokensForSchemaFree(pipelineRepoName, worklow, ak, sk)
	autoLogdbTokens := GenerateTokensForAutoLogdb(pipelineRepoName, logdbRepoName, ak, sk)
	autoTsdbTokens := GenerateTokensForAutoTSDB(pipelineRepoName, tsdbRepoName, ak, sk)
	seriesToken, serisCreateExportTokn, seriesUpdateExportToken, getExportTokens := GenerateTokensForTSDBSeries(pipelineRepoName, tsdbRepoName, ak, sk, []string{"s1"})
	autoKodoTokens := GenerateTokensForAutoKodo(pipelineRepoName, bucketName, ak, sk)

	var err error

	fmt.Println(autoTsdbTokens[pdTSDB+"-"+base.OpCreateRepo])

	postDataInput := &pipeline.SchemaFreeInput{
		SchemaFreeToken: pipeline.SchemaFreeToken{
			PipelineCreateRepoToken:        schematokens[base.OpCreateRepo],
			PipelinePostDataToken:          schematokens[base.OpPostData],
			PipelineGetRepoToken:           schematokens[base.OpGetRepo],
			PipelineUpdateRepoToken:        schematokens[base.OpUpdateRepo],
			PipelineGetWorkflowToken:       schematokens[base.OpGetWorkflow],
			PipelineCreateWorkflowToken:    schematokens[base.OpCreateWorkflow],
			PipelineStartWorkflowToken:     schematokens[base.OpStartWorkflow],
			PipelineStopWorkflowToken:      schematokens[base.OpStopWorkflow],
			PipelineGetWorkflowStatusToken: schematokens[base.OpGetWorkflowStatus],
		},
		RepoName: pipelineRepoName,
		Datas: pipeline.Datas{
			pipeline.Data{
				"f1": "12.7",
				"f2": 1.0,
				"f4": 123,
				"f5": true,
			},
			pipeline.Data{
				"f1": "12.7",
				"f2": 1.0,
				"f4": 123,
				"f7": "x1",
			},
			pipeline.Data{
				"f1": "12.7",
				"f2": 1.0,
				"f4": 123,
				"f8": "x1",
			},
		},
		Option: &pipeline.SchemaFreeOption{
			ToLogDB: true,
			AutoExportToLogDBInput: pipeline.AutoExportToLogDBInput{
				RepoName:    pipelineRepoName,
				LogRepoName: logdbRepoName,
				Retention:   "3d",
				AutoExportLogDBTokens: pipeline.AutoExportLogDBTokens{
					PipelineCreateRepoToken: autoLogdbTokens[pdPipeline+"-"+base.OpCreateRepo],
					PipelineGetRepoToken:    autoLogdbTokens[pdPipeline+"-"+base.OpGetRepo],
					CreateLogDBRepoToken:    autoLogdbTokens[pdLogdb+"-"+base.OpCreateRepo],
					UpdateLogDBRepoToken:    autoLogdbTokens[pdLogdb+"-"+base.OpUpdateRepo],
					GetLogDBRepoToken:       autoLogdbTokens[pdLogdb+"-"+base.OpGetRepo],
					CreateExportToken:       autoLogdbTokens[pdPipeline+"-"+base.OpCreateExport],
					UpdateExportToken:       autoLogdbTokens[pdPipeline+"-"+base.OpUpdateExport],
					GetExportToken:          autoLogdbTokens[pdPipeline+"-"+base.OpGetExport],
					ListExportToken:         autoLogdbTokens[pdPipeline+"-"+base.OpListExports],
				},
			},
			ToTSDB: true,
			AutoExportToTSDBInput: pipeline.AutoExportToTSDBInput{
				RepoName:     pipelineRepoName,
				TSDBRepoName: tsdbRepoName,
				Retention:    "3d",
				SeriesName:   "s1",
				SeriesTags:   make(map[string][]string),
				AutoExportTSDBTokens: pipeline.AutoExportTSDBTokens{
					PipelineGetRepoToken:   autoTsdbTokens[pdPipeline+"-"+base.OpGetRepo],
					CreateTSDBRepoToken:    autoTsdbTokens[pdTSDB+"-"+base.OpCreateRepo],
					CreateTSDBSeriesTokens: seriesToken,
					CreateExportToken:      serisCreateExportTokn,
					UpdateExportToken:      seriesUpdateExportToken,
					GetExportToken:         getExportTokens,
					ListExportToken:        autoTsdbTokens[pdPipeline+"-"+base.OpListExports],
				},
			},
			ToKODO: true,
			AutoExportToKODOInput: pipeline.AutoExportToKODOInput{
				RepoName:   pipelineRepoName,
				BucketName: bucketName,
				Email:      "sunjianbo@qiniu.com",
				Retention:  2,
				AutoExportKodoTokens: pipeline.AutoExportKodoTokens{
					PipelineGetRepoToken: autoKodoTokens[base.OpGetRepo],
					CreateExportToken:    autoKodoTokens[base.OpCreateExport],
					UpdateExportToken:    autoKodoTokens[base.OpUpdateExport],
					GetExportToken:       autoKodoTokens[base.OpGetExport],
					ListExportToken:      autoKodoTokens[base.OpListExports],
				},
			},
		},
	}
	_, err = client.PostDataSchemaFree(postDataInput)
	if err != nil {
		t.Error(err)
	}

}
