package pipeline

import (
	"bytes"
	"os"
	"time"

	. "github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	. "github.com/qiniu/pandora-go-sdk/base/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	. "github.com/qiniu/pandora-go-sdk/pipeline"
)

var (
	cfg               *config.Config
	client            PipelineAPI
	region            = "<Region>"
	endpoint          = config.DefaultPipelineEndpoint
	ak                = "<AccessKey>"
	sk                = "<SecretKey>"
	logger            Logger
	defaultRepoSchema []RepoSchemaEntry
	defaultContainer  *Container
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
		logger.Error("new pipeline client failed, err: %v", err)
	}

	defaultRepoSchema = []RepoSchemaEntry{
		RepoSchemaEntry{
			Key:       "f1",
			ValueType: "string",
			Required:  true,
		},
		RepoSchemaEntry{
			Key:       "f2",
			ValueType: "float",
			Required:  true,
		},
	}
	defaultContainer = &Container{
		Type:  "M16C4",
		Count: 1,
	}

}

func writeToFile(path string, content []byte) {
	file, err := os.Create(path)
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close()
	if _, err = file.Write(content); err != nil {
		logger.Error(err)
		return
	}
}

func Sample_Group() {
	groupName := "group"
	createInput := &CreateGroupInput{
		GroupName:       groupName,
		Container:       defaultContainer,
		Region:          region,
		AllocateOnStart: false,
	}
	err := client.CreateGroup(createInput)
	if err != nil {
		logger.Error(err)
		return
	}
	getOutput, err := client.GetGroup(&GetGroupInput{GroupName: groupName})
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(getOutput)

	listOutput, err := client.ListGroups(&ListGroupsInput{})
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(listOutput)

	err = client.StartGroupTask(&StartGroupTaskInput{GroupName: groupName})
	if err != nil {
		logger.Error(err)
		return
	}

	err = client.StopGroupTask(&StopGroupTaskInput{GroupName: groupName})
	if err != nil {
		logger.Error(err)
		return
	}

	err = client.DeleteGroup(&DeleteGroupInput{GroupName: groupName})
	if err != nil {
		logger.Error(err)
		return
	}
}

func Sample_Repo() {
	repoName := "repo"
	createInput := &CreateRepoInput{
		RepoName:  repoName,
		Region:    region,
		Schema:    defaultRepoSchema,
		GroupName: "",
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

func Sample_Plugin() {
	pluginName := "plugin"
	pluginInput := &UploadPluginInput{
		PluginName: pluginName,
		Buffer:     bytes.NewBufferString("plugin content"),
	}
	// 从buffer中上传plugin
	err := client.UploadPlugin(pluginInput)
	if err != nil {
		logger.Error(err)
		return
	}

	getOutput, err := client.GetPlugin(&GetPluginInput{PluginName: pluginName})
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(getOutput)

	listOutput, err := client.ListPlugins(&ListPluginsInput{})
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(listOutput)

	if err = client.DeletePlugin(&DeletePluginInput{PluginName: pluginName}); err != nil {
		logger.Error(err)
		return
	}

	path := "/tmp/plugin.jar"
	filePluginInput := &UploadPluginFromFileInput{
		PluginName: "plugin",
		FilePath:   path,
	}
	content := []byte("local file plugin content")
	writeToFile(path, content)

	// 从本地文件中上传plugin
	if err = client.UploadPluginFromFile(filePluginInput); err != nil {
		logger.Error(err)
		return
	}
	if err = client.DeletePlugin(&DeletePluginInput{PluginName: pluginName}); err != nil {
		logger.Error(err)
		return
	}
}

func Sample_PostData() {
	repoName := "repo_post_data"
	createRepoInput := &CreateRepoInput{
		RepoName: repoName,
		Schema:   defaultRepoSchema,
		Region:   region,
	}
	err := client.CreateRepo(createRepoInput)
	if err != nil {
		logger.Error(err)
		return
	}

	// 使用Points结构打点
	postDataInput := &PostDataInput{
		RepoName: repoName,
		Points: Points{
			Point{
				[]PointField{
					PointField{
						Key:   "f1",
						Value: 12.7,
					},
					PointField{
						Key:   "f2",
						Value: 1.0,
					},
				},
			},
		},
	}
	err = client.PostData(postDataInput)
	if err != nil {
		logger.Error(err)
		return
	}

	// 使用Buffer打点
	buf := []byte("f1=\"12.7\"\tf2=3.14\nf1=\"dang\"\tf2=1024.0")
	postDataFromBytesInput := &PostDataFromBytesInput{
		RepoName: repoName,
		Buffer:   buf,
	}
	err = client.PostDataFromBytes(postDataFromBytesInput)
	if err != nil {
		logger.Error(err)
		return
	}

	// 使用reader传递数据打点
	postDataFromReaderInput := &PostDataFromReaderInput{
		RepoName: repoName,
		Reader:   bytes.NewReader(buf),
	}
	err = client.PostDataFromReader(postDataFromReaderInput)
	if err != nil {
		logger.Error(err)
		return
	}

	// 从文件中读取数据打点
	path := "/tmp/postdata"
	writeToFile(path, buf)
	postDataFromFileInput := &PostDataFromFileInput{
		RepoName: repoName,
		FilePath: path,
	}
	err = client.PostDataFromFile(postDataFromFileInput)
	if err != nil {
		logger.Error(err)
		return
	}

	err = client.DeleteRepo(&DeleteRepoInput{RepoName: repoName})
	if err != nil {
		logger.Error(err)
		return
	}
}

func TestTransform() {
	createRepoInput := &CreateRepoInput{
		RepoName: "src_repo",
		Schema:   defaultRepoSchema,
		Region:   region,
	}
	err := client.CreateRepo(createRepoInput)
	if err != nil {
		logger.Error(err)
		return
	}

	spec := &TransformSpec{
		Mode:      "sql",
		Code:      "select * from stream",
		Interval:  "5m",
		Container: defaultContainer,
	}
	createTransInput := &CreateTransformInput{
		SrcRepoName:   "src_repo",
		DestRepoName:  "dest_repo",
		TransformName: "transform",
		Spec:          spec,
	}
	err = client.CreateTransform(createTransInput)
	if err != nil {
		logger.Error(err)
		return
	}

	listTransOutput, err := client.ListTransforms(&ListTransformsInput{RepoName: "src_repo"})
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(listTransOutput)

	getTransOutput, err := client.GetTransform(&GetTransformInput{RepoName: "src_repo", TransformName: "transform"})
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(getTransOutput)

	err = client.DeleteTransform(&DeleteTransformInput{RepoName: "src_repo", TransformName: "transform"})
	if err != nil {
		logger.Error(err)
		return
	}

	err = client.DeleteRepo(&DeleteRepoInput{RepoName: "src_repo"})
	if err != nil {
		logger.Error(err)
		return
	}
}

func Sample_Export() {
	repoName := "repo_for_export"
	createRepoInput := &CreateRepoInput{
		RepoName: repoName,
		Schema:   defaultRepoSchema,
		Region:   region,
	}
	err := client.CreateRepo(createRepoInput)
	if err != nil {
		logger.Error(err)
		return
	}

	// 在Input里面不需要指定Type，sdk会自动填充
	exports := []CreateExportInput{
		CreateExportInput{
			RepoName:   repoName,
			ExportName: "tsdb_export",
			Spec: &ExportTsdbSpec{
				DestRepoName: "tsdb_dest_repo",
				SeriesName:   "series",
				Tags:         map[string]string{"tag1": "#f1"},
				Fields:       map[string]string{"field1": "#f1", "field2": "#f2"},
			},
			Whence: "oldest",
		},
		CreateExportInput{
			RepoName:   repoName,
			ExportName: "lg_export",
			Spec: &ExportLogDBSpec{
				DestRepoName: "lg_dest_repo",
				Doc:          map[string]interface{}{"f1": "#f1"},
			},
			Whence: "newest",
		},
		CreateExportInput{
			RepoName:   repoName,
			ExportName: "mongo_export",
			Spec: &ExportMongoSpec{
				Host:     "10.200.20.23:27017",
				DbName:   "test",
				CollName: "my_coll",
				Mode:     "UPSERT",
				Doc:      map[string]interface{}{"f1": "#f1"},
			},
		},
		CreateExportInput{
			RepoName:   repoName,
			ExportName: "kodo_export",
			Spec: &ExportKodoSpec{
				Bucket:         "mybucket",
				KeyPrefix:      "export_prefix_",
				Fields:         map[string]string{"field1": "#f1", "field2": "#f2"},
				RotateInterval: 30,
			},
		},
		CreateExportInput{
			RepoName:   repoName,
			ExportName: "http_export",
			Spec: &ExportHttpSpec{
				Host: "http://qiniu.com",
				Uri:  "/resource",
			},
		},
	}

	for _, export := range exports {
		err = client.CreateExport(&export)
		if err != nil {
			logger.Errorf("export: %s create failed, err: %v", export.ExportName, err)
		}

		getExportInput := &GetExportInput{
			RepoName:   export.RepoName,
			ExportName: export.ExportName,
		}
		getExportOutput, err := client.GetExport(getExportInput)
		if err != nil {
			logger.Error(err)
			return
		}
		logger.Info(getExportOutput)
	}

	listExportsOutput, err := client.ListExports(&ListExportsInput{RepoName: repoName})
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Info(listExportsOutput)

	for _, export := range exports {
		deleteExportInput := &DeleteExportInput{
			RepoName:   export.RepoName,
			ExportName: export.ExportName,
		}
		err = client.DeleteExport(deleteExportInput)
		if err != nil {
			logger.Error(err)
			return
		}
	}

	err = client.DeleteRepo(&DeleteRepoInput{RepoName: repoName})
	if err != nil {
		logger.Error(err)
		return
	}
}

func Sample_PostDataWithToken() {
	repoName := "repo_post_data_with_token"
	createInput := &CreateRepoInput{
		RepoName: repoName,
		Region:   region,
		Schema:   defaultRepoSchema,
	}

	err := client.CreateRepo(createInput)
	if err != nil {
		logger.Error(err)
		return
	}

	// client创建一个对repo_post_data_with_token这个repo进行打点的token，它的过期时间是当前时间再加10s
	// 也就是说10秒钟之后再用这个token去打点会收到401 Unauthorized错误
	td := &TokenDesc{}
	td.Expires = time.Now().Unix() + 10
	td.Method = "POST"
	td.Url = "/v2/repos/repo_post_data_with_token/data"
	td.ContentType = "text/plain"

	token, err := client.MakeToken(td)
	if err != nil {
		logger.Error(err)
		return
	}

	cfg2 := NewConfig().WithEndpoint(endpoint)

	client2, err2 := New(cfg2)
	if err2 != nil {
		logger.Error("new pipeline client failed, err: %v", err2)
		return
	}
	postDataInput := &PostDataInput{
		RepoName: repoName,
		Points: Points{
			Point{
				[]PointField{
					PointField{
						Key:   "f1",
						Value: 12.7,
					},
					PointField{
						Key:   "f2",
						Value: 1.0,
					},
				},
			},
		},
		PandoraToken: PandoraToken{
			Token: token,
		},
	}
	// client2在打点的时候拿到的postDataInput里面包含了client1刚刚给签发的token
	err = client2.PostData(postDataInput)
	if err != nil {
		logger.Error(err)
		return
	}

	time.Sleep(15 * time.Second)

	// 15秒之后再用这个token去访问，就会鉴权不通过
	err = client2.PostData(postDataInput)
	if err == nil {
		logger.Errorf("expired token: %s, expires: %d, now: %d", token, td.Expires, time.Now().Unix())
		return
	}

	err = client.DeleteRepo(&DeleteRepoInput{RepoName: repoName})
	if err != nil {
		logger.Error(err)
		return
	}
}

func Sample_HowToUsePipelineSelfDefinedError() {
	createRepo := func() {
		logger.Info("create repo")
	}
	createTransform := func() {
		logger.Info("create transform")
	}
	sleepAndRetry := func() {
		logger.Info("sleep and retry")
	}
	checkAuth := func() {
		logger.Info("check auth")
	}
	postData := func() {
		logger.Info("post data")
	}
	printErrorToScreen := func() {
		logger.Info("print error to screen")
	}

	output, err := client.GetTransform(&GetTransformInput{RepoName: "repo", TransformName: "transform"})
	if err == nil { // 没有出错，做一些处理
		logger.Info(output)
		postData()
		return
	}
	// 根据具体的错误分别做处理
	v, ok := err.(*reqerr.RequestError)
	if !ok {
		logger.Info("this error isn't a RequestError")
	}
	switch v.ErrorType {
	case reqerr.NoSuchRepoError:
		createRepo()
	case reqerr.NoSuchTransformError:
		createTransform()
	case reqerr.InternalServerError:
		sleepAndRetry()
	case reqerr.UnauthorizedError:
		checkAuth()
	case reqerr.DefaultRequestError:
		printErrorToScreen()
	default:
		logger.Error(v)
		printErrorToScreen()
	}
}
