package pipeline

import (
	"bytes"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"fmt"

	. "github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	"github.com/qiniu/pandora-go-sdk/pipeline"
)

var (
	cfg               *config.Config
	client            pipeline.PipelineAPI
	region            = os.Getenv("REGION")
	endpoint          = os.Getenv("PIPELINE_HOST")
	ak                = os.Getenv("ACCESS_KEY")
	sk                = os.Getenv("SECRET_KEY")
	logger            Logger
	defaultRepoSchema []pipeline.RepoSchemaEntry
	defaultContainer  *pipeline.Container
)

func init() {
	var err error
	logger = NewDefaultLogger()
	cfg = pipeline.NewConfig().
		WithEndpoint(endpoint).
		WithAccessKeySecretKey(ak, sk).
		WithLogger(logger).
		WithLoggerLevel(LogDebug)

	client, err = pipeline.New(cfg)
	if err != nil {
		logger.Errorf("new pipeline client failed, err: %v", err)
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
		Type:  "M16C4",
		Count: 1,
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

func TestGroup(t *testing.T) {
	groupName := "group"
	createInput := &pipeline.CreateGroupInput{
		GroupName:       groupName,
		Container:       defaultContainer,
		Region:          region,
		AllocateOnStart: false,
	}
	err := client.CreateGroup(createInput)
	if err != nil {
		t.Error(err)
	}

	getOutput, err := client.GetGroup(&pipeline.GetGroupInput{GroupName: groupName})
	if err != nil {
		t.Error(err)
	}
	if getOutput == nil {
		t.Errorf("get output should not be empty")
	}

	if getOutput.Region != region {
		t.Errorf("region of group should be nb")
	}
	if getOutput.Container.Count != defaultContainer.Count ||
		getOutput.Container.Type != defaultContainer.Type {
		t.Errorf("container of group %v should equal to default container %v", getOutput.Container, defaultContainer)
	}
	if getOutput.CreateTime == "" || getOutput.UpdateTime == "" {
		t.Errorf("create time and update time should not be empty")
	}

	listOutput, err := client.ListGroups(&pipeline.ListGroupsInput{})
	if err != nil {
		t.Error(err)
	}
	if listOutput == nil {
		t.Error("listOutput should not be empty")
	}
	if len(listOutput.Groups) != 1 {
		t.Errorf("group count should be 1 but %d", len(listOutput.Groups))
	}

	err = client.StartGroupTask(&pipeline.StartGroupTaskInput{GroupName: groupName})
	if err != nil {
		t.Error(err)
	}

	err = client.StopGroupTask(&pipeline.StopGroupTaskInput{GroupName: groupName})
	if err != nil {
		t.Error(err)
	}

	err = client.DeleteGroup(&pipeline.DeleteGroupInput{GroupName: groupName})
	if err != nil {
		t.Error(err)
	}
}

func TestRepo(t *testing.T) {
	repoName := "repo"
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
	if len(listOutput.Repos) != 1 {
		t.Errorf("repo count should be 1 but %d", len(listOutput.Repos))
	}
	if listOutput.Repos[0].RepoName != "repo" {
		t.Error("repo name is different to origin name")
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
		WithLoggerLevel(LogDebug).
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
		WithLoggerLevel(LogDebug).
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
		WithLoggerLevel(LogDebug).
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
	repoName := "repo_for_export"
	createRepoInput := &pipeline.CreateRepoInput{
		RepoName: repoName,
		Schema:   defaultRepoSchema,
		Region:   "nb",
	}
	err := client.CreateRepo(createRepoInput)
	if err != nil {
		t.Error(err)
	}

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
			ExportName: "lg_export",
			Spec: &pipeline.ExportLogDBSpec{
				DestRepoName: "lg_dest_repo",
				Doc:          map[string]interface{}{"f1": "#f1"},
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

	td := &TokenDesc{}
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
		PipelineToken: pipeline.PipelineToken{
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
