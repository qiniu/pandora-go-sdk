package request

import (
	"strings"
	"testing"

	"github.com/qiniu/pandora-go-sdk/base/config"
)

func BenchmarkGzip(b *testing.B) {
	req := New(&config.Config{Gzip: true}, nil, &Operation{Method: "POST"}, "", nil, nil)
	var s string
	for i := 0; i < 2048; i++ {
		s += "a"
	}
	r := strings.NewReader(s)
	for i := 0; i < b.N; i++ {
		req.SetReaderBody(r)
	}
	//	fmt.Println(req.bodyLength)
}

func BenchmarkNoGzip(b *testing.B) {
	req := New(&config.Config{Gzip: false}, nil, &Operation{Method: "POST"}, "", nil, nil)
	var s string
	for i := 0; i < 2048; i++ {
		s += "a"
	}
	r := strings.NewReader(s)
	for i := 0; i < b.N; i++ {
		req.SetReaderBody(r)
	}
	//	fmt.Println(req.bodyLength)
}
