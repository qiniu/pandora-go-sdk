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
