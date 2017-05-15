package request

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

type Request struct {
	Config           *config.Config
	Operation        *Operation
	HTTPClient       *http.Client
	HTTPRequest      *http.Request
	HTTPResponse     *http.Response
	Body             io.ReadSeeker
	Error            error
	Data             interface{}
	RequestId        string
	Headers          map[string]string
	EnableContentMD5 bool
	Logger           base.Logger
	token            string
	errBuilder       reqerr.ErrBuilder
}

type Operation struct {
	Name   string
	Method string
	Path   string
}

func New(cfg *config.Config, client *http.Client, op *Operation, token string, errBuilder reqerr.ErrBuilder, data interface{}) *Request {
	httpReq, _ := http.NewRequest(op.Method, "", nil)
	var err error
	httpReq.URL, err = url.Parse(cfg.Endpoint + op.Path)
	if err != nil {
		cfg.Logger.Errorf("parse url failed, err: %v", err)
		return nil
	}
	httpReq.Host = httpReq.URL.Host

	logger := cfg.Logger
	if logger == nil {
		logger = base.NewDefaultLogger()
		logger.SetLoggerLevel(base.LogFatal)
	}

	r := &Request{
		Config:      cfg,
		Operation:   op,
		HTTPClient:  client,
		HTTPRequest: httpReq,
		Body:        nil,
		Error:       err,
		Data:        data,
		Headers:     map[string]string{},
		Logger:      logger,
		token:       token,
		errBuilder:  errBuilder,
	}

	return r
}

func (r *Request) handleBody() {
	switch v := r.Body.(type) {
	case *os.File:
		r.HTTPRequest.ContentLength = r.fileSize(v)
		if r.Error != nil {
			return
		}
		if r.md5Sum(v); r.Error != nil {
			return
		}
	case io.ReadSeeker:
		start, _ := v.Seek(0, 1)
		end, _ := v.Seek(0, 2)
		r.HTTPRequest.ContentLength = end - start
		if _, r.Error = v.Seek(0, 0); r.Error != nil {
			return
		}
		if r.md5Sum(v); r.Error != nil {
			return
		}
	case lener:
		r.HTTPRequest.ContentLength = int64(v.Len())
	case nil:
		break
	}
}

type lener interface {
	Len() int64
}

func (r *Request) fileSize(f *os.File) int64 {
	stat, err := f.Stat()
	r.Error = err
	return stat.Size()
}

func (r *Request) md5Sum(reader io.ReadSeeker) {
	if !r.EnableContentMD5 {
		return
	}

	hash := md5.New()
	_, err := io.Copy(hash, reader)
	if err != nil {
		r.Error = fmt.Errorf("failed to read body, err: %v", err)
	}
	_, err = reader.Seek(0, 0)
	if err != nil {
		r.Error = fmt.Errorf("failed to seek body, err: %v", err)
	}
	sum := hash.Sum(nil)
	sum64 := make([]byte, base64.StdEncoding.EncodedLen(len(sum)))
	base64.StdEncoding.Encode(sum64, sum)
	r.HTTPRequest.Header.Set(base.HTTPHeaderContentMD5, string(sum64))
	return
}

func (r *Request) SetBufferBody(buf []byte) {
	r.SetReaderBody(bytes.NewReader(buf))
}

func (r *Request) SetStringBody(s string) {
	r.SetReaderBody(strings.NewReader(s))
}

func (r *Request) SetVariantBody(v interface{}) error {
	if !reflect.ValueOf(v).Elem().IsValid() {
		r.Error = fmt.Errorf("invalid interface %#v", v)
		return r.Error
	}
	vv, ok := v.(base.Validator)
	if !ok {
		r.Error = fmt.Errorf("invalid type cast, cannot cast to validator")
		r.Logger.Error(logFormatter(r, "cast to validator"))
		return r.Error
	}
	if r.Error = vv.Validate(); r.Error != nil {
		r.Logger.Error(logFormatter(r, "validate input"))
		return r.Error
	}

	var buf []byte
	buf, r.Error = json.Marshal(v)
	r.SetBufferBody(buf)

	return nil
}

func (r *Request) SetReaderBody(reader io.ReadSeeker) {
	reader.Seek(0, 0)
	r.HTTPRequest.Body = newOffsetReader(reader, 0)
	r.Body = reader
}

func (r *Request) EnableContentMD5d() {
	r.EnableContentMD5 = true
}

func (r *Request) SetHeader(k, v string) {
	r.Headers[k] = v
}

func (r *Request) build() {
	for k, v := range r.Headers {
		r.HTTPRequest.Header.Set(k, v)
	}

	r.HTTPRequest.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))

	r.handleBody()
	if r.Error != nil {
		return
	}

	if r.token != "" {
		r.HTTPRequest.Header.Set("Authorization", r.token)
		return
	}

	r.Error = base.Sign(r.Config.Ak, r.Config.Sk, r.HTTPRequest)
	if r.Error != nil {
		return
	}
}

func (r *Request) Send() error {
	r.build()
	if r.Error != nil {
		r.Logger.Error(logFormatter(r, "build request"))
		return r.Error
	}

	r.HTTPResponse, r.Error = r.HTTPClient.Do(r.HTTPRequest)
	if r.Error != nil {
		r.Logger.Error(logFormatter(r, "send request"))
		return r.Error
	}

	buf := r.readResponse()
	if r.Error != nil {
		r.Logger.Error(logFormatter(r, "read response"))
		return r.Error
	}
	if r.HTTPResponse.StatusCode == 200 {
		r.unmarshal(buf)
	} else {
		if r.HTTPResponse.Header.Get(base.HTTPHeaderContentType) != "application/json" {
			r.Error = r.errBuilder.Build(string(buf),
				string(buf),
				r.HTTPResponse.Header.Get(base.HTTPHeaderRequestId),
				r.HTTPResponse.StatusCode)
			r.Logger.Error(logFormatter(r, "receive non-json response"))
			return r.Error
		}
		r.unmarshalError(buf)
	}
	return r.Error
}

func (r *Request) unmarshal(buf []byte) {
	if r.Data == nil ||
		(reflect.ValueOf(r.Data).Kind() != reflect.Slice &&
			!reflect.ValueOf(r.Data).Elem().IsValid()) {
		return
	}
	r.Error = json.Unmarshal(buf, &r.Data)
	if r.Error != nil {
		r.Logger.Error(logFormatter(r, fmt.Sprintf("unmarshal response: %s", string(buf))))
		return
	}
}

func (r *Request) unmarshalError(buf []byte) {
	var err reqerr.RequestError
	if len(buf) > 0 {
		err1 := json.Unmarshal(buf, &err)
		if err1 != nil {
			r.Error = err1
			r.Logger.Error(logFormatter(r, "unmarshal error"))
			return
		}
	} else {
		buf = make([]byte, 0)
	}
	r.Error = r.errBuilder.Build(err.Message,
		string(buf),
		r.HTTPResponse.Header.Get(base.HTTPHeaderRequestId),
		r.HTTPResponse.StatusCode)
}

func (r *Request) readResponse() (out []byte) {
	defer r.HTTPResponse.Body.Close()
	out, r.Error = ioutil.ReadAll(r.HTTPResponse.Body)
	if r.Error == io.EOF || r.Error == io.ErrUnexpectedEOF {
		r.Error = nil
		return
	}
	return
}

func logFormatter(r *Request, stage string) string {
	return fmt.Sprintf("%s failed, operation %s, error %v",
		stage, r.Operation.Name, r.Error)
}
