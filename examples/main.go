package main

import (
	"fmt"
	"time"

	"github.com/qiniu/pandora-go-sdk/v2/auth"
	"github.com/qiniu/pandora-go-sdk/v2/conf"
	"github.com/qiniu/pandora-go-sdk/v2/search"
)

func main() {
	cfg := conf.NewConfg([]string{"http://localhost:8080"})
	credentials := auth.New("<Your Pandora Token>")
	param := search.NewSearchParam(
		`repo="matrix" | stats count() by host`, // SPL
		"fast",                                  // 搜索模式
		0,                                       // 起始时间
		1629200879919,                           // 截止时间
		-1,                                      // 限制返回数据，-1 代表不限制
		false,                                   // 非预览模式
	)
	m := search.NewSearchManager(credentials, cfg)

	job, jobInfo, jobResult, err := m.CreateAndWaitForQueryResults(param, time.Second, time.Second*10)
	fmt.Printf("job %v\n jobInfo %v \n jobResult %v \nerr %v\n", job, jobInfo, jobResult, err)
}
