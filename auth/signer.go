package auth

// Pandora Token 类型
// PandoraToken, AppSessionKey
type TokenType int

const (
	PandoraToken TokenType = iota
	AppSessionKey
)
