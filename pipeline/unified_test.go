package pipeline

import (
	"strconv"
	"testing"

	"github.com/qiniu/pandora-go-sdk/logdb"
	"github.com/stretchr/testify/assert"
)

func TestAutoExport(t *testing.T) {
	xxx := map[string]interface{}{
		"abc": "123",
	}
	/*
	   kodoExportSpecStr := `
	   	"bucket":"bucket1",
	   		"email": "abc@qiniu.com",
	   		"accessKey": "ak",
	   		"fields": {
	   			"key1": "#key1",
	   			"key2": "#key2",
	   		},
	   			"format": "parquet",
	   			"retention": 30
	   		}`
	*/
	_, ok := xxx["xsxs"].(string)
	assert.Equal(t, ok, false)

}

func TestGetSeriesName(t *testing.T) {
	seriesTag := map[string][]string{
		"a":     []string{},
		"a_b":   []string{},
		"a_b_c": []string{},
	}
	series := getSeriesName(seriesTag, "a")
	assert.Equal(t, series, "")
	series = getSeriesName(seriesTag, "a_")
	assert.Equal(t, series, "")
	series = getSeriesName(seriesTag, "b")
	assert.Equal(t, series, "")
	series = getSeriesName(seriesTag, "a__")
	assert.Equal(t, series, "a")
	series = getSeriesName(seriesTag, "a__b")
	assert.Equal(t, series, "a")
	series = getSeriesName(seriesTag, "a__b__c")
	assert.Equal(t, series, "a")
	series = getSeriesName(seriesTag, "a_b__c")
	assert.Equal(t, series, "a_b")
	series = getSeriesName(seriesTag, "a_b_c__d__e")
	assert.Equal(t, series, "a_b_c")
	series = getSeriesName(seriesTag, "a_b___c_ddd")
	assert.Equal(t, series, "a_b")

	seriesTag = map[string][]string{
		"cpu":             []string{},
		"system":          []string{},
		"processes":       []string{},
		"netstat":         []string{},
		"net":             []string{},
		"mem":             []string{},
		"swap":            []string{},
		"kernel_vmstat":   []string{},
		"kernel":          []string{},
		"disk":            []string{},
		"diskio":          []string{},
		"linux_sysctl_fs": []string{},
	}

	series = getSeriesName(seriesTag, "system__load1")
	assert.Equal(t, "system", series)
	series = getSeriesName(seriesTag, "system__n_users")
	assert.Equal(t, "system", series)
	series = getSeriesName(seriesTag, "processes__total")
	assert.Equal(t, "processes", series)
	series = getSeriesName(seriesTag, "processes__total_threads")
	assert.Equal(t, "processes", series)
	series = getSeriesName(seriesTag, "netstat__tcp_syn_sent")
	assert.Equal(t, "netstat", series)
	series = getSeriesName(seriesTag, "netstat__tcp_none")
	assert.Equal(t, "netstat", series)
	series = getSeriesName(seriesTag, "net__err_in")
	assert.Equal(t, "net", series)
	series = getSeriesName(seriesTag, "net__packets_recv")
	assert.Equal(t, "net", series)
	series = getSeriesName(seriesTag, "mem__total")
	assert.Equal(t, "mem", series)
	series = getSeriesName(seriesTag, "mem__available_percent")
	assert.Equal(t, "mem", series)
	series = getSeriesName(seriesTag, "swap__total")
	assert.Equal(t, "swap", series)
	series = getSeriesName(seriesTag, "swap__used_percent")
	assert.Equal(t, "swap", series)
	series = getSeriesName(seriesTag, "cpu__usage_cpu")
	assert.Equal(t, "cpu", series)
	series = getSeriesName(seriesTag, "cpu__time_cpu")
	assert.Equal(t, "cpu", series)
	series = getSeriesName(seriesTag, "kernel_vmstat__kswapd_inodesteal")
	assert.Equal(t, "kernel_vmstat", series)
	series = getSeriesName(seriesTag, "kernel_vmstat__thp_zero_page_alloc_failed")
	assert.Equal(t, "kernel_vmstat", series)
	series = getSeriesName(seriesTag, "linux_sysctl_fs__super_nr")
	assert.Equal(t, "linux_sysctl_fs", series)
	series = getSeriesName(seriesTag, "linux_sysctl_fs__inode_preshrink_nr")
	assert.Equal(t, "linux_sysctl_fs", series)
	series = getSeriesName(seriesTag, "kernel__context_switches")
	assert.Equal(t, "kernel", series)
	series = getSeriesName(seriesTag, "kernel__interrupts")
	assert.Equal(t, "kernel", series)
	series = getSeriesName(seriesTag, "disk__free")
	assert.Equal(t, "disk", series)
	series = getSeriesName(seriesTag, "disk__inodes_free")
	assert.Equal(t, "disk", series)
	series = getSeriesName(seriesTag, "diskio__read_time")
	assert.Equal(t, "diskio", series)
	series = getSeriesName(seriesTag, "diskio__name")
	assert.Equal(t, "diskio", series)
}

func TestConvertSchema2LogDB_Analyzer(t *testing.T) {
	testData := []struct {
		analyzer  AnalyzerInfo
		schemas   []RepoSchemaEntry
		expSchema []logdb.RepoSchemaEntry
	}{
		{
			// Analyzer 包括所有字段
			analyzer: AnalyzerInfo{
				Analyzer: map[string]string{
					"a": logdb.KeyWordAnalyzer,
					"b": logdb.DicAnajAnalyzer,
					"c": logdb.AnsjAnalyzer,
					"d": logdb.SimpleAnalyzer,
					"e": logdb.StandardAnalyzer,
					"f": logdb.StopAnalyzer,
					"g": logdb.KeyWordAnalyzer,
				},
			},
			schemas: []RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "e",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "f",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "g",
					ValueType: PandoraTypeString,
				},
			},
			expSchema: []logdb.RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.KeyWordAnalyzer,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.DicAnajAnalyzer,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.AnsjAnalyzer,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.SimpleAnalyzer,
				},
				{
					Key:       "e",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StandardAnalyzer,
				},
				{
					Key:       "f",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StopAnalyzer,
				},
				{
					Key:       "g",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.KeyWordAnalyzer,
				},
			},
		},
		{
			// Analyzer 包括部分字段
			// 其余的为默认的 标准分词
			analyzer: AnalyzerInfo{
				Analyzer: map[string]string{
					"a": logdb.KeyWordAnalyzer,
					"b": logdb.DicAnajAnalyzer,
					"c": logdb.AnsjAnalyzer,
				},
			},
			schemas: []RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
				},
			},
			expSchema: []logdb.RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.KeyWordAnalyzer,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.DicAnajAnalyzer,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.AnsjAnalyzer,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StandardAnalyzer,
				},
			},
		},
		{
			// Analyzer 包括部分字段
			// 其余的为 Default 指定的分词
			analyzer: AnalyzerInfo{
				Default: logdb.StopAnalyzer,
				Analyzer: map[string]string{
					"a": logdb.KeyWordAnalyzer,
					"b": logdb.DicAnajAnalyzer,
					"c": logdb.AnsjAnalyzer,
				},
			},
			schemas: []RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
				},
			},
			expSchema: []logdb.RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.KeyWordAnalyzer,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.DicAnajAnalyzer,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.AnsjAnalyzer,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StopAnalyzer,
				},
			},
		},
		{
			// Analyzer 为 nil
			// 所有字段为 Default 指定的分词
			analyzer: AnalyzerInfo{
				Default:  logdb.StopAnalyzer,
				Analyzer: nil,
			},
			schemas: []RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
				},
			},
			expSchema: []logdb.RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StopAnalyzer,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StopAnalyzer,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StopAnalyzer,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StopAnalyzer,
				},
			},
		},
		{
			// Analyzer 为 nil
			// Default 为空
			// 所有字段均为 标准分词
			analyzer: AnalyzerInfo{},
			schemas: []RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
				},
			},
			expSchema: []logdb.RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StandardAnalyzer,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StandardAnalyzer,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StandardAnalyzer,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StandardAnalyzer,
				},
			},
		},
		{
			analyzer: AnalyzerInfo{
				Default: "aaaaa",
				Analyzer: map[string]string{
					"a": "aa",
					"b": "bb",
					"c": "cc",
				},
			},
			schemas: []RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
				},
			},
			expSchema: []logdb.RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StandardAnalyzer,
				},
				{
					Key:       "b",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StandardAnalyzer,
				},
				{
					Key:       "c",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StandardAnalyzer,
				},
				{
					Key:       "d",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.StandardAnalyzer,
				},
			},
		},
		{
			// Analyzer 包括所有字段
			analyzer: AnalyzerInfo{
				Analyzer: map[string]string{
					"a.d":   logdb.SimpleAnalyzer,
					"a.e":   logdb.StandardAnalyzer,
					"b.f":   logdb.StopAnalyzer,
					"c.g.h": logdb.KeyWordAnalyzer,
					"i":     logdb.KeyWordAnalyzer,
				},
			},
			schemas: []RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: PandoraTypeMap,
					Schema: []RepoSchemaEntry{
						{
							Key:       "d",
							ValueType: PandoraTypeString,
						},
						{
							Key:       "e",
							ValueType: PandoraTypeString,
						},
					},
				},
				{
					Key:       "b",
					ValueType: PandoraTypeMap,
					Schema: []RepoSchemaEntry{
						{
							Key:       "f",
							ValueType: PandoraTypeString,
						},
					},
				},
				{
					Key:       "c",
					ValueType: PandoraTypeMap,
					Schema: []RepoSchemaEntry{
						{
							Key:       "g",
							ValueType: PandoraTypeMap,
							Schema: []RepoSchemaEntry{
								{
									Key:       "h",
									ValueType: PandoraTypeString,
								},
							},
						},
					},
				},
				{
					Key:       "i",
					ValueType: PandoraTypeString,
				},
			},
			expSchema: []logdb.RepoSchemaEntry{
				{
					Key:       "a",
					ValueType: logdb.TypeObject,
					Schemas: []logdb.RepoSchemaEntry{
						{
							Key:       "d",
							ValueType: PandoraTypeString,
							Analyzer:  logdb.StopAnalyzer,
						},
						{
							Key:       "e",
							ValueType: PandoraTypeString,
							Analyzer:  logdb.StopAnalyzer,
						},
					},
				},
				{
					Key:       "b",
					ValueType: logdb.TypeObject,
					Schemas: []logdb.RepoSchemaEntry{
						{
							Key:       "f",
							ValueType: PandoraTypeString,
							Analyzer:  logdb.StopAnalyzer,
						},
					},
				},
				{
					Key:       "c",
					ValueType: logdb.TypeObject,
					Schemas: []logdb.RepoSchemaEntry{
						{
							Key:       "g",
							ValueType: logdb.TypeObject,
							Schemas: []logdb.RepoSchemaEntry{
								{
									Key:       "h",
									ValueType: PandoraTypeString,
									Analyzer:  logdb.KeyWordAnalyzer,
								},
							},
						},
					},
				},
				{
					Key:       "i",
					ValueType: PandoraTypeString,
					Analyzer:  logdb.KeyWordAnalyzer,
				},
			},
		},
	}

	for _, td := range testData {
		gotSchema := convertSchema2LogDB(td.schemas, td.analyzer, nil)
		if len(gotSchema) != len(td.expSchema) {
			t.Fatalf("got schema number error, exp %v, but got %v", len(td.expSchema), len(gotSchema))
		}
		for i, ret := range gotSchema {
			assert.Equal(t, td.expSchema[i].Key, ret.Key)
			assert.Equal(t, td.expSchema[i].Analyzer, ret.Analyzer)
			assert.Equal(t, td.expSchema[i].ValueType, ret.ValueType)
		}
	}
}

var BenchConvertRet []logdb.RepoSchemaEntry

func BenchmarkConvertSchema2LogDB(b *testing.B) {
	analyzers := map[string]string{}
	schemas := []RepoSchemaEntry{}
	expSchemas := []logdb.RepoSchemaEntry{}
	for i := 0; i < 26; i++ {
		schema := RepoSchemaEntry{
			Key:       strconv.Itoa(i),
			ValueType: PandoraTypeMap,
		}
		expSchema := logdb.RepoSchemaEntry{
			Key:       strconv.Itoa(i),
			ValueType: logdb.TypeObject,
		}
		for j := 0; j < 1000; j++ {
			analyzers[strconv.Itoa(i)+"."+strconv.Itoa(j)] = logdb.KeyWordAnalyzer
			schema.Schema = append(schema.Schema, RepoSchemaEntry{
				Key:       strconv.Itoa(j),
				ValueType: PandoraTypeString,
			})
			expSchema.Schemas = append(expSchema.Schemas, logdb.RepoSchemaEntry{
				Key:       strconv.Itoa(j),
				ValueType: PandoraTypeString,
				Analyzer:  logdb.KeyWordAnalyzer,
			})
		}
		schemas = append(schemas, schema)
		expSchemas = append(expSchemas, expSchema)
	}
	testData := []struct {
		analyzer  AnalyzerInfo
		schemas   []RepoSchemaEntry
		expSchema []logdb.RepoSchemaEntry
	}{
		{
			// Analyzer 包括所有字段
			analyzer: AnalyzerInfo{
				Analyzer: analyzers,
			},
			schemas:   schemas,
			expSchema: expSchemas,
		},
	}
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, td := range testData {
			BenchConvertRet = convertSchema2LogDB(td.schemas, td.analyzer, nil)
		}
	}
}
