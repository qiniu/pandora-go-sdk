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
		`repo="matrix" | stats count() by host`, "fast",
		0,
		1629200879919,
		-1,
		false,
	)
	fmt.Println(param)
	m := search.NewSearchManager(credentials, cfg)

	job, jobInfo, jobResult, err := m.CreateAndWaitForQueryResults(param, time.Second, time.Second*10)
	fmt.Printf("job %v\n jobInfo %v \n jobResult %v \nerr %v\n", job, jobInfo, jobResult, err)
}
