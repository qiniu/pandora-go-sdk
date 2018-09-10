package pipeline

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/stretchr/testify/assert"
)

func TestUnpack(t *testing.T) {
	cfg := NewConfig().
		WithEndpoint(config.DefaultPipelineEndpoint)

	client, err := NewDefaultClient(cfg)
	if err != nil {
		t.Error(err)
	}
	repoName := "test"
	schemas := map[string]RepoSchemaEntry{}

	schemas["abcd"] = RepoSchemaEntry{
		Key:       "abcd",
		ValueType: "string",
	}
	client.repoSchemas[repoName] = schemas
	d := Data{}
	// 一个点有10个byte: "abcd=efgh\n"
	d["abcd"] = "efgh"

	// 在最坏的情况下（每条数据都超过最大限制），则要求每个point都只包含一条数据
	PandoraMaxBatchSize = 0
	datas := []Data{}
	for i := 0; i < 3; i++ {
		datas = append(datas, d)
	}
	contexts, err := client.unpack(&SchemaFreeInput{
		RepoName: repoName,
		Datas:    Datas(datas),
		NoUpdate: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, len(contexts), 3)
	for _, c := range contexts {
		assert.Equal(t, len(c.inputs.Buffer), 10)
	}

	// Pandora的2MB限制
	PandoraMaxBatchSize = 2 * 1024 * 1024
	datas = []Data{}
	for i := 0; i < 2*1024*102; i++ {
		datas = append(datas, d)
	}
	contexts, err = client.unpack(&SchemaFreeInput{
		RepoName: repoName,
		Datas:    Datas(datas),
		NoUpdate: true,
	})
	assert.NoError(t, err)

	assert.Equal(t, len(contexts), 1)
	assert.Equal(t, len(contexts[0].inputs.Buffer), 2*1024*1020)

	// Pandora的2MB限制
	PandoraMaxBatchSize = 2 * 1024 * 1024
	datas = []Data{}
	for i := 0; i < 2*1024*103; i++ {
		datas = append(datas, d)
	}
	contexts, err = client.unpack(&SchemaFreeInput{
		RepoName: repoName,
		Datas:    Datas(datas),
		NoUpdate: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, len(contexts), 2)
	assert.Equal(t, len(contexts[0].datas), 2*1024*1024/10)
	assert.Equal(t, len(contexts[1].datas), 2*1024*103-2*1024*1024/10)
	// 第一个包是最大限制以内的最大整十数
	assert.Equal(t, len(contexts[0].inputs.Buffer), 2*1024*1024/10*10)
	// 第二个包是总bytes 减去第一个包的数量
	assert.Equal(t, len(contexts[1].inputs.Buffer), 2*1024*103*10-2*1024*1024/10*10)
}

func TestBuff(t *testing.T) {
	var buff bytes.Buffer
	buff.Write([]byte("12345678"))
	bt0 := buff.Bytes()
	bt1 := make([]byte, buff.Len())
	copy(bt1, buff.Bytes())
	buff.Truncate(0)
	fmt.Println(string(bt0), string(bt1))
	buff.Write([]byte("xxxxx"))
	fmt.Println(string(bt0), string(bt1))

	/* 输出
	12345678 12345678
	xxxxx678 12345678
	*/
}

func Test_checkExportUpdate(t *testing.T) {
	tests := []struct {
		spec         map[string]interface{}
		ipConfig     *LocateIPConfig
		expectUpdate bool
		expectConfig *LocateIPConfig
	}{
		{
			expectUpdate: false,
		},
		{
			spec: map[string]interface{}{
				"locateIPConfig": &LocateIPConfig{
					ShouldLocateIP: true,
					Mappings: map[string]*LocateIPDetails{
						"a": {
							ShouldLocateField: true,
							FieldNames: map[string]string{
								IPFieldNameCountry: "a" + IPFiledSuffixCountry,
							},
							WantedFields: map[string]bool{
								IPWantCountry: true,
							},
						},
					},
				},
			},
			ipConfig:     nil,
			expectUpdate: false,
			expectConfig: &LocateIPConfig{
				ShouldLocateIP: true,
				Mappings: map[string]*LocateIPDetails{
					"a": {
						ShouldLocateField: true,
						FieldNames: map[string]string{
							IPFieldNameCountry: "a" + IPFiledSuffixCountry,
						},
						WantedFields: map[string]bool{
							IPWantCountry: true,
						},
					},
				},
			},
		},
		{
			spec: nil,
			ipConfig: &LocateIPConfig{
				ShouldLocateIP: true,
				Mappings: map[string]*LocateIPDetails{
					"a": {
						ShouldLocateField: true,
						FieldNames: map[string]string{
							IPFieldNameCountry: "a" + IPFiledSuffixCountry,
						},
						WantedFields: map[string]bool{
							IPWantCountry: true,
						},
					},
				},
			},
			expectUpdate: true,
			expectConfig: &LocateIPConfig{
				ShouldLocateIP: true,
				Mappings: map[string]*LocateIPDetails{
					"a": {
						ShouldLocateField: true,
						FieldNames: map[string]string{
							IPFieldNameCountry: "a" + IPFiledSuffixCountry,
						},
						WantedFields: map[string]bool{
							IPWantCountry: true,
						},
					},
				},
			},
		},
		{
			spec: map[string]interface{}{"locateIPConfig": nil},
			ipConfig: &LocateIPConfig{
				ShouldLocateIP: true,
				Mappings: map[string]*LocateIPDetails{
					"a": {
						ShouldLocateField: true,
						FieldNames: map[string]string{
							IPFieldNameCountry: "a" + IPFiledSuffixCountry,
						},
						WantedFields: map[string]bool{
							IPWantCountry: true,
						},
					},
				},
			},
			expectUpdate: true,
			expectConfig: &LocateIPConfig{
				ShouldLocateIP: true,
				Mappings: map[string]*LocateIPDetails{
					"a": {
						ShouldLocateField: true,
						FieldNames: map[string]string{
							IPFieldNameCountry: "a" + IPFiledSuffixCountry,
						},
						WantedFields: map[string]bool{
							IPWantCountry: true,
						},
					},
				},
			},
		},
		{
			spec: map[string]interface{}{"locateIPConfig": &LocateIPConfig{}},
			ipConfig: &LocateIPConfig{
				ShouldLocateIP: true,
				Mappings: map[string]*LocateIPDetails{
					"a": {
						ShouldLocateField: true,
						FieldNames: map[string]string{
							IPFieldNameCountry: "a" + IPFiledSuffixCountry,
						},
						WantedFields: map[string]bool{
							IPWantCountry: true,
						},
					},
				},
			},
			expectUpdate: true,
			expectConfig: &LocateIPConfig{
				ShouldLocateIP: true,
				Mappings: map[string]*LocateIPDetails{
					"a": &LocateIPDetails{
						ShouldLocateField: true,
						FieldNames: map[string]string{
							IPFieldNameCountry: "a" + IPFiledSuffixCountry,
						},
						WantedFields: map[string]bool{
							IPWantCountry: true,
						},
					},
				},
			},
		},
		{
			spec: map[string]interface{}{
				"locateIPConfig": &LocateIPConfig{
					ShouldLocateIP: true,
					Mappings: map[string]*LocateIPDetails{
						"a": {
							ShouldLocateField: true,
							FieldNames: map[string]string{
								IPFieldNameCountry: "a" + IPFiledSuffixCountry,
								IPFieldNameCity:    "a" + IPFiledSuffixCity,
							},
							WantedFields: map[string]bool{
								IPWantCountry: true,
								IPWantCity:    true,
							},
						},
					},
				},
			},
			ipConfig: &LocateIPConfig{
				ShouldLocateIP: true,
				Mappings: map[string]*LocateIPDetails{
					"a": {
						ShouldLocateField: true,
						FieldNames: map[string]string{
							IPFieldNameCountry: "a" + IPFiledSuffixCountry,
							IPFieldNameRegion:  "a" + IPFiledSuffixRegion,
						},
						WantedFields: map[string]bool{
							IPWantCountry: true,
							IPWantRegion:  true,
						},
					},
				},
			},
			expectUpdate: true,
			expectConfig: &LocateIPConfig{
				ShouldLocateIP: true,
				Mappings: map[string]*LocateIPDetails{
					"a": {
						ShouldLocateField: true,
						FieldNames: map[string]string{
							IPFieldNameCountry: "a" + IPFiledSuffixCountry,
							IPFieldNameCity:    "a" + IPFiledSuffixCity,
							IPFieldNameRegion:  "a" + IPFiledSuffixRegion,
						},
						WantedFields: map[string]bool{
							IPWantCountry: true,
							IPWantRegion:  true,
							IPWantCity:    true,
						},
					},
				},
			},
		},
		{
			spec: map[string]interface{}{
				"locateIPConfig": &LocateIPConfig{
					ShouldLocateIP: true,
					Mappings: map[string]*LocateIPDetails{
						"a": {
							ShouldLocateField: true,
							FieldNames: map[string]string{
								IPFieldNameCountry: "a" + IPFiledSuffixCountry,
							},
							WantedFields: map[string]bool{
								IPWantCountry: true,
							},
						},
					},
				},
			},
			ipConfig: &LocateIPConfig{
				ShouldLocateIP: true,
				Mappings: map[string]*LocateIPDetails{
					"a": {
						ShouldLocateField: true,
						FieldNames: map[string]string{
							IPFieldNameCountry: "a" + IPFiledSuffixCountry,
						},
						WantedFields: map[string]bool{
							IPWantCountry: true,
						},
					},
				},
			},
			expectUpdate: false,
			expectConfig: &LocateIPConfig{
				ShouldLocateIP: true,
				Mappings: map[string]*LocateIPDetails{
					"a": {
						ShouldLocateField: true,
						FieldNames: map[string]string{
							IPFieldNameCountry: "a" + IPFiledSuffixCountry,
						},
						WantedFields: map[string]bool{
							IPWantCountry: true,
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		res, update := checkExportUpdate(test.spec, test.ipConfig)
		assert.Equal(t, test.expectUpdate, update)
		if update {
			if res == nil {
				assert.Equal(t, test.ipConfig, res)
				continue
			}
			assert.Equal(t, test.expectConfig.ShouldLocateIP, res.ShouldLocateIP)
			assert.Equal(t, test.expectConfig.Mappings["a"].ShouldLocateField, res.Mappings["a"].ShouldLocateField)
			assert.Equal(t, test.expectConfig.Mappings["a"].FieldNames, res.Mappings["a"].FieldNames)
			assert.Equal(t, test.expectConfig.Mappings["a"].WantedFields, res.Mappings["a"].WantedFields)
		}
	}
}

func Test_checkUpdateFiledName(t *testing.T) {
	tests := []struct {
		filedsName       map[string]string
		locateFiledsName map[string]string
		update           bool
		expectFiledName  map[string]string
	}{
		{update: false},
		{
			filedsName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
			},
			locateFiledsName: nil,
			update:           true,
			expectFiledName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
			},
		},
		{
			filedsName: nil,
			locateFiledsName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
			},
			update: false,
			expectFiledName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
			},
		},
		{
			filedsName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
			},
			locateFiledsName: map[string]string{
				IPFieldNameCity: "a" + IPFiledSuffixCity,
			},
			update: true,
			expectFiledName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
				IPFieldNameCity:    "a" + IPFiledSuffixCity,
			},
		},
		{
			filedsName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
			},
			locateFiledsName: map[string]string{},
			update:           true,
			expectFiledName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
			},
		},
		{
			filedsName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
			},
			locateFiledsName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
			},
			update: false,
			expectFiledName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
			},
		},
		{
			filedsName: map[string]string{
				IPFieldNameCountry: "b" + IPFiledSuffixCountry,
			},
			locateFiledsName: map[string]string{
				IPFieldNameCountry: "a" + IPFiledSuffixCountry,
			},
			update: true,
			expectFiledName: map[string]string{
				IPFieldNameCountry: "b" + IPFiledSuffixCountry,
			},
		},
	}

	for _, test := range tests {
		res, update := checkUpdateFiledName(test.filedsName, test.locateFiledsName)
		assert.Equal(t, test.update, update)
		assert.Equal(t, test.expectFiledName, res)
	}
}

func Test_checkUpdateWantedFiled(t *testing.T) {
	tests := []struct {
		wantedField       map[string]bool
		locateWantedField map[string]bool
		update            bool
		expectWantedField map[string]bool
	}{
		{update: false},
		{
			wantedField: map[string]bool{
				IPWantCountry: true,
			},
			locateWantedField: map[string]bool{
				IPWantCity: true,
			},
			update: true,
			expectWantedField: map[string]bool{
				IPWantCountry: true,
				IPWantCity:    true,
			},
		},
		{
			wantedField: map[string]bool{},
			locateWantedField: map[string]bool{
				IPWantCity: true,
			},
			update: false,
			expectWantedField: map[string]bool{
				IPWantCity: true,
			},
		},
		{
			wantedField: map[string]bool{
				IPWantCity: true,
			},
			locateWantedField: map[string]bool{},
			update:            true,
			expectWantedField: map[string]bool{
				IPWantCity: true,
			},
		},
		{
			wantedField: nil,
			locateWantedField: map[string]bool{
				IPWantCity: true,
			},
			update: false,
			expectWantedField: map[string]bool{
				IPWantCity: true,
			},
		},
	}

	for _, test := range tests {
		res, update := checkUpdateWantedFiled(test.wantedField, test.locateWantedField)
		assert.Equal(t, test.update, update)
		assert.Equal(t, test.expectWantedField, res)
	}
}
