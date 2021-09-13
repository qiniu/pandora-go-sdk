package conf

import "fmt"

const Version = "0.0.1"

const (
	CONTENT_TYPE_JSON      = "application/json"
	CONTENT_TYPE_FORM      = "application/x-www-form-urlencoded"
	CONTENT_TYPE_OCTET     = "application/octet-stream"
	CONTENT_TYPE_MULTIPART = "multipart/form-data"
)

type Config struct {
	// Pandora 入口
	PandoraHosts []string `json:"hosts,omitempty"`

	// Api root path, 默认为 /api/v1
	ApiRootPath string `json:"api_root_path,omitempty"`

	// App api root path, 默认为 /custom/v1
	AppApiRootPath string `json:"app_api_root_path,omitempty"`

	// 如果设置的Host本身是以http://开头的，又设置了该字段为true，那么优先使用该字段，使用https协议
	// 同理如果该字段为false, 但是设置的host以https开头，那么使用http协议通信
	UseHTTPS bool //是否使用https域名
}

func NewConfg(hosts []string) *Config {
	return &Config{
		PandoraHosts:   hosts,
		ApiRootPath:    "/api/v1",
		AppApiRootPath: "/custom/v1",
	}
}

func (c *Config) GetReqUrl() string {
	h := c.PandoraHosts[0]
	return fmt.Sprintf("%s%s", h, c.ApiRootPath)
}
