package tracing

import (
	"context"
	"fmt"
	"net/http"
)

const (
	SERVICE_TYPE   = "SERVICE_TYPE"
	ST_STORAGE     = 1000
	ST_KODO        = 1000
	ST_ES          = 1000
	ST_APP_SERVICE = 1020
	ST_LB_SERVICE  = 1020
	ST_APP_METHOD  = 1021
	ST_JETTY       = 1030
	ST_MONGODB     = 2650
	ST_ZK          = 99002
	ST_QCONF       = 99001
	ST_KAFKA       = 8660
	ST_RPC         = 9060

	CALLED_BY    = "CalledBy"
	ERROR        = "Error"
	MONGO_SERVER = "MongoServer"
	MONGO_DBNAME = "DBName"
	ES_SERVER    = "EsServer"
	ZK_SERVER    = "ZkServer"
	REMOTE_ADDR  = "RemoteAddr"
)

func HttpHandleDec(h func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	var resultFunc = func(w http.ResponseWriter, req *http.Request) {
		opName := fmt.Sprintf("%s:%s", req.Method, req.URL.String())
		span := HttpAPISpanStart(req, opName)
		span.SetTags(map[string]interface{}{
			HTTP_METHOD:  req.Method,
			HTTP_URL:     req.URL,
			SERVICE_TYPE: ST_APP_SERVICE})
		defer span.Complete()
		ctx := span.ContextInit()
		goId := CurGoroutineID()
		PushContext(goId, ctx)
		defer PopContext(goId)
		h(w, req)
	}
	if isTracing {
		return resultFunc
	} else {
		return h
	}
}

type httpDoFunc func(req *http.Request) (*http.Response, error)

func HttpDoDec(doFunc httpDoFunc, opName string) httpDoFunc {
	var resultFunc = func(req *http.Request) (*http.Response, error) {
		_, span := APISpanStart(opName)
		defer span.Complete()
		span.BeforeHttpRequest(req)
		resp, err := doFunc(req)
		span.SetTags(map[string]interface{}{"Host": req.Host, SERVICE_TYPE: ST_APP_SERVICE, ERROR: err})
		if resp != nil {
			span.SetTags(map[string]interface{}{STATUS_CODE: resp.StatusCode})
		}
		return resp, err
	}
	if isTracing {
		return resultFunc
	} else {
		return doFunc
	}
}

type getFunc func(string) (*http.Response, error)

func HttpGetDec(getF getFunc, opName string) getFunc {
	var resultFunc = func(url string) (*http.Response, error) {
		_, span := APISpanStart(opName)
		defer span.Complete()
		resp, err := getF(url)
		span.SetTags(map[string]interface{}{"URL": url, SERVICE_TYPE: ST_APP_SERVICE, ERROR: err})
		if resp != nil {
			span.SetTags(map[string]interface{}{STATUS_CODE: resp.StatusCode})
		}
		return resp, err
	}
	if isTracing {
		return resultFunc
	} else {
		return getF
	}
}

// simpleFunc wrapper user defined functions and return tags map for span
type simpleFunc func() map[string]interface{}

func SimpleAPIDec(opName string, simpleF simpleFunc) {
	var tags map[string]interface{}
	if isTracing {
		goId, span := APISpanStart(opName)
		defer span.Complete()
		ctx := span.ContextInit()
		PushContext(goId, ctx)
		defer PopContext(goId)
		span.SetTag(SERVICE_TYPE, ST_APP_METHOD)
		defer span.SetTags(tags)
	}
	tags = simpleF()
}

func SimpleAPIWithCtxDec(opName string, pCtx context.Context, simpleF simpleFunc) {
	var tags map[string]interface{}
	if isTracing {
		goId, span := APISpanStartWithCtx(opName, pCtx)
		defer span.Complete()
		ctx := span.ContextInit()
		PushContext(goId, ctx)
		defer PopContext(goId)
		span.SetTag(SERVICE_TYPE, ST_APP_METHOD)
		defer span.SetTags(tags)
	}
	tags = simpleF()
}

func GoRoutineDec(opName string, simpleF simpleFunc) {
	if isTracing {
		goId := CurGoroutineID()
		topCtx := TopContext(goId)
		go SimpleAPIWithCtxDec(opName, topCtx, simpleF)
		return
	}
	go simpleF()
}
