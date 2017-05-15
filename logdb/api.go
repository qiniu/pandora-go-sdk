package logdb

import (
	"net/url"

	. "github.com/qiniu/pandora-go-sdk/base"
)

func (c *Logdb) CreateRepo(input *CreateRepoInput) (err error) {
	op := c.newOperation(OpCreateRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Logdb) CreateRepoFromDSL(input *CreateRepoDSLInput) (err error) {
	schemas, err := toSchema(input.DSL, 0)
	if err != nil {
		return
	}
	return c.CreateRepo(&CreateRepoInput{
		LogdbToken: input.LogdbToken,
		RepoName:   input.RepoName,
		Region:     input.Region,
		Retention:  input.Retention,
		Schema:     schemas,
	})
}

func (c *Logdb) UpdateRepo(input *UpdateRepoInput) (err error) {
	op := c.newOperation(OpUpdateRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Logdb) GetRepo(input *GetRepoInput) (output *GetRepoOutput, err error) {
	op := c.newOperation(OpGetRepo, input.RepoName)

	output = &GetRepoOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Logdb) ListRepos(input *ListReposInput) (output *ListReposOutput, err error) {
	op := c.newOperation(OpListRepos)

	output = &ListReposOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Logdb) DeleteRepo(input *DeleteRepoInput) (err error) {
	op := c.newOperation(OpDeleteRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Logdb) SendLog(input *SendLogInput) (output *SendLogOutput, err error) {
	op := c.newOperation(OpSendLog, input.RepoName, input.OmitInvalidLog)

	output = &SendLogOutput{}
	req := c.newRequest(op, input.Token, &output)
	buf, err := input.Logs.Buf()
	if err != nil {
		return
	}
	req.SetBufferBody(buf)
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return output, req.Send()
}

func (c *Logdb) QueryLog(input *QueryLogInput) (output *QueryLogOutput, err error) {
	var highlight bool
	if input.Highlight != nil {
		highlight = true
	}
	op := c.newOperation(OpQueryLog, input.RepoName, url.QueryEscape(input.Query), input.Sort, input.From, input.Size, highlight)

	output = &QueryLogOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.Highlight != nil {
		if err = req.SetVariantBody(input.Highlight); err != nil {
			return
		}
		req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	}
	return output, req.Send()
}

func (c *Logdb) QueryHistogramLog(input *QueryHistogramLogInput) (output *QueryHistogramLogOutput, err error) {
	op := c.newOperation(OpQueryHistogramLog, input.RepoName, url.QueryEscape(input.Query), input.From, input.To, input.Field)

	output = &QueryHistogramLogOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Logdb) MakeToken(desc *TokenDesc) (string, error) {
	return MakeTokenInternal(c.Config.Ak, c.Config.Sk, desc)
}
