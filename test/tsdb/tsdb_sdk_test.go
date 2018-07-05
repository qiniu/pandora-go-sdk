package tsdb

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	. "github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	. "github.com/qiniu/pandora-go-sdk/tsdb"
)

var (
	cfg      *config.Config
	client   TsdbAPI
	region   = os.Getenv("REGION")
	endpoint = os.Getenv("TSDB_HOST")
	ak       = os.Getenv("ACCESS_KEY")
	sk       = os.Getenv("SECRET_KEY")
	logger   Logger
)

func init() {
	var err error

	if region == "" {
		region = "nb"
	}

	if endpoint == "" {
		endpoint = config.DefaultTSDBEndpoint
	}

	if ak == "" || sk == "" {
		err = fmt.Errorf("ak/sk should not be empty")
		log.Println(err)
		return
	}

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
}

func TestRepo(t *testing.T) {
	repoName := "tsdb_sdk_test_repo"
	//create repo
	err := client.CreateRepo(&CreateRepoInput{
		RepoName: repoName,
		Region:   region,
	})
	if err != nil {
		t.Fatal("create repo fail: ", err)
	}

	//ensure repo created
	repo, err := client.GetRepo(&GetRepoInput{RepoName: repoName})
	if err != nil {
		t.Fatal("get repo fail: ", err)
	}
	if repo.RepoName != repoName {
		t.Fatal("repoName does not match")
	}

	//list repo
	repos, err := client.ListRepos(&ListReposInput{})
	if err != nil {
		t.Error(err)
	}
	t.Log(repos)

	//update metadata
	metadate := map[string]string{"key1": "val1"}
	err = client.UpdateRepoMetadata(&UpdateRepoMetadataInput{
		RepoName: repoName,
		Metadata: metadate,
	})
	if err != nil {
		t.Fatal("update repo metadata fail: ", err)
	}

	//ensure updated
	repo, err = client.GetRepo(&GetRepoInput{RepoName: repoName})
	if err != nil {
		t.Fatal("get repo fail: ", err)
	}
	if !reflect.DeepEqual(repo.Metadata, metadate) {
		t.Fatal("update repo metadata fail: ", err)
	}

	//delete metadata
	err = client.DeleteRepoMetadata(&DeleteRepoMetadataInput{RepoName: repoName})
	if err != nil {
		t.Fatal("delete repo metadata fail: ", err)
	}
	//ensure deleted
	repo, err = client.GetRepo(&GetRepoInput{RepoName: repoName})
	if err != nil {
		t.Fatal("get repo fail: ", err)
	}
	exp := map[string]string{}
	exp = nil
	if !reflect.DeepEqual(repo.Metadata, exp) {
		t.Fatal("delete repo metadata fail: ", spew.Sdump(repo.Metadata), spew.Sdump(exp))
	}

	//delete repo
	err = client.DeleteRepo(&DeleteRepoInput{RepoName: repoName})
	if err != nil {
		t.Fatal("delete repo fail: ", err)
	}
	//ensure deleted
	repo, err = client.GetRepo(&GetRepoInput{RepoName: repoName})
	if err != nil {
		t.Fatal("get repo fail: ", err)
	}
	if repo.Deleting != "true" {
		t.Fatal("delete repo metadata fail: ", err)
	}

}

func TestSeries(t *testing.T) {

	repoName := "tsdb_sdk_test_series_repo"
	seriesName := "tsdb_sdk_test_series"
	retention := "oneDay"

	//create repo
	err := client.CreateRepo(&CreateRepoInput{
		RepoName: repoName,
		Region:   region,
	})
	if err != nil {
		t.Fatal("create repo fail: ", err)
	}

	//create series
	err = client.CreateSeries(&CreateSeriesInput{
		RepoName:   repoName,
		SeriesName: seriesName,
		Retention:  retention,
	})
	if err != nil {
		t.Fatal("create series fail: ", err)
	}

	//ensure created && list series
	series, err := client.ListSeries(&ListSeriesInput{
		RepoName: repoName,
	})
	if err != nil {
		t.Fatal("list series fail: ", err)
	}
	if len(*series) != 1 {
		t.Fatal("series length not match, exp: 1 got:%v", len(*series))
	}

	//update metadata
	metadata := map[string]string{
		"key1": "val1",
	}
	err = client.UpdateSeriesMetadata(&UpdateSeriesMetadataInput{
		RepoName:   repoName,
		SeriesName: seriesName,
		Metadata:   metadata,
	})
	if err != nil {
		t.Fatal("list series fail: ", err)
	}

	//ensure updated
	series, err = client.ListSeries(&ListSeriesInput{
		RepoName: repoName,
	})
	if err != nil {
		t.Fatal("list series fail: ", err)
	}
	if len(*series) != 1 {
		t.Fatal("series length not match, exp: 1 got:%v", len(*series))
	}
	if !reflect.DeepEqual((*series)[0].Metadata, metadata) {
		t.Fatalf("update metadata fail: got:%v,exp:%v", (*series)[0].Metadata, metadata)
	}

	//delete metadata
	err = client.DeleteSeriesMetadata(&DeleteSeriesMetadataInput{
		RepoName:   repoName,
		SeriesName: seriesName,
	})
	if err != nil {
		t.Fatal("delete series metadata fail: ", err)
	}

	//delete series
	err = client.DeleteSeries(&DeleteSeriesInput{
		RepoName:   repoName,
		SeriesName: seriesName,
	})
	if err != nil {
		t.Fatal("delete series metadata fail: ", err)
	}

}

func TestView(t *testing.T) {

	repoName := "tsdb_sdk_test_view_repo"
	seriesName := "tsdb_sdk_test_view_series"
	viewName := "tsdb_sdk_test_view"
	retention := "oneDay"

	//create repo
	err := client.CreateRepo(&CreateRepoInput{
		RepoName: repoName,
		Region:   region,
	})
	if err != nil {
		t.Fatal("create repo fail: ", err)
	}

	//create series
	err = client.CreateSeries(&CreateSeriesInput{
		RepoName:   repoName,
		SeriesName: seriesName,
		Retention:  retention,
	})
	if err != nil {
		t.Fatal("create series fail: ", err)
	}

	//create view
	err = client.CreateView(&CreateViewInput{
		ViewName:  viewName,
		RepoName:  repoName,
		Sql:       "select count(value) into tsdb_sdk_test_view from tsdb_sdk_test_view_series group by time(1m)",
		Retention: retention,
	})
	if err != nil {
		t.Fatal("create view fail: %v", err)
	}

	//ensure created
	_, err = client.GetView(&GetViewInput{
		RepoName: repoName,
		ViewName: viewName,
	})
	if err != nil {
		t.Fatal("create view fail: %v", err)
	}

	//list view
	_, err = client.ListView(&ListViewInput{
		RepoName: repoName,
	})
	if err != nil {
		t.Fatal("list view fail: %v", err)
	}

	//delete view
	err = client.DeleteView(&DeleteViewInput{
		RepoName: repoName,
		ViewName: viewName,
	})
	if err != nil {
		t.Fatal("delete view fail: %v", err)
	}

	//ensure deleted
	view, err := client.GetView(&GetViewInput{
		RepoName: repoName,
		ViewName: viewName,
	})
	if view.Deleting != "true" {
		t.Fatal("delete view fail: %v", err)
	}

}
func TestWriteAndQuery(t *testing.T) {
	repoName := "tsdb_sdk_test_points_repo"
	seriesName := "tsdb_sdk_test_points_series"
	retention := "oneDay"

	//create repo
	err := client.CreateRepo(&CreateRepoInput{
		RepoName: repoName,
		Region:   region,
	})
	if err != nil {
		t.Fatal("create repo fail: ", err)
	}

	//create series
	err = client.CreateSeries(&CreateSeriesInput{
		RepoName:   repoName,
		SeriesName: seriesName,
		Retention:  retention,
	})
	if err != nil {
		t.Fatal("create series fail: ", err)
	}
	//write points
	p := Point{
		SeriesName: seriesName,
		Tags: map[string]string{
			"host":   "h1",
			"region": "region1",
		},
		Fields: map[string]interface{}{
			"value": 123,
		},
	}
	err = client.PostPoints(&PostPointsInput{
		RepoName: repoName,
		Points:   Points{p},
	})
	if err != nil {
		t.Fatalf("post points fail: %v", err)
	}
	//query points
	ret, err := client.QueryPoints(&QueryInput{
		RepoName: repoName,
		Sql:      fmt.Sprintf("select * from %s", seriesName),
	})
	if err != nil {
		t.Fatalf("query points fail: %v", err)
	}
	exp := &QueryOutput{}
	if !reflect.DeepEqual(ret, exp) {
		t.Fatal("query points fail\ngot:%v\nexp:%v\n", ret, exp)
	}
}

func TestPoint(t *testing.T) {
	p := Point{
		SeriesName: "cpu",
		Tags: map[string]string{
			"host":   "h1",
			"region": "region1",
		},
		Fields: map[string]interface{}{
			"value": 123,
		},
	}
	expect := "cpu,host=h1,region=region1 value=123i"
	if expect != p.String() {
		t.Error("parse point fail, got:%s\nexp:%s\n", p.String(), expect)
	}

	ps := Points{
		Point{
			SeriesName: "cpu",
			Tags: map[string]string{
				"host":   "h1",
				"region": "region1",
			},
			Fields: map[string]interface{}{
				"value": 123,
			},
		},
		Point{
			SeriesName: "cpu",
			Tags: map[string]string{
				"host":   "h1",
				"region": "region1",
			},
			Fields: map[string]interface{}{
				"value": 123,
			},
			Time: 123,
		},
	}

	got := ps.Buffer()
	exp := []byte(`cpu,host=h1,region=region1 value=123i
cpu,host=h1,region=region1 value=123i 123`)
	if !reflect.DeepEqual(got, exp) {
		t.Errorf("parse point fail, got:%s\nexp:%s\n", string(got), string(exp))
	}
}
