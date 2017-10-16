package pipeline

import (
	"testing"

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
