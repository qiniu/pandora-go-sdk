package auth

import (
	"net/http"
)

//  七牛鉴权类，用于生成Qbox, Qiniu, Upload签名
// AK/SK可以从 https://portal.qiniu.com/user/key 获取。
type Credentials struct {
	Token string
}

// 构建一个Credentials对象
func New(token string) *Credentials {
	return &Credentials{token}
}

// SignToken 根据t的类型对请求进行签名，并把token加入req中
func (ath *Credentials) AddToken(t TokenType, req *http.Request) error {
	switch t {
	case AppSessionKey:
		req.Header.Add("X-App-Session-Key", ath.Token)
	default:
		req.Header.Add("Authorization", ath.Token)
	}
	return nil
}
