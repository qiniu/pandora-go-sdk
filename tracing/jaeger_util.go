package tracing

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"gopkg.in/yaml.v2"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"

	qLog "github.com/qiniu/log"
)

var (
	tracer     opentracing.Tracer
	closer     io.Closer
	mapLock    *sync.RWMutex
	contextMap map[uint64]*stack
	isTracing  = false
)

const (
	STATUS_CODE                  = "http.status.code"
	HTTP_METHOD                  = "http.method"
	HTTP_URL                     = "http.url"
	DEFAULT_SIMPLING_PROBABILITY = 0.001
	// fetch the second function before current function, in current call chain. used by GetCallerOpName()
	// with call chain A ---> B ---> C, if C is current function, then CALLER_SKIP_STEP_2 points to A
	CALLER_SKIP_STEP_2 = 2
	// fetch the third function before current function, in function calling traces
	CALLER_SKIP_STEP_3 = 3
	CALLER_SKIP_STEP_4 = 4
)

type JaegerSpan struct {
	opentracing.Span
}

func GetServiceName() (service string) {
	file, _ := exec.LookPath(os.Args[0])
	fileItems := strings.Split(file, "/")
	lens := len(fileItems)
	if lens >= 1 {
		service = fileItems[lens-1]
	} else {
		service = "UNKNOWN"
	}
	return
}

func genDefaultCfg(cfg *config.Configuration) {
	cfg.Sampler = &config.SamplerConfig{
		Type:  jaeger.SamplerTypeProbabilistic,
		Param: DEFAULT_SIMPLING_PROBABILITY,
	}
	cfg.Reporter = &config.ReporterConfig{
		CollectorEndpoint: "http://127.0.0.1:14268/api/traces",
		LogSpans:          true,
	}
	cfg.Disabled = false
}

func cfgToString(cfg *config.Configuration) string {
	return fmt.Sprintf("{Sampler:{Type:%s,Param:%f}, Reporter:%s}",
		cfg.Sampler.Type, cfg.Sampler.Param, cfg.Reporter.CollectorEndpoint)
}

func ServiceInit() {
	configFile := "jaeger_client.yaml"
	jCliBytes, err := ioutil.ReadFile(configFile)
	var cfg = new(config.Configuration)
	if err != nil {
		if !os.IsNotExist(err) {
			qLog.Errorf("open %s failed! %v", configFile, err)
			return
		}
		qLog.Warnf("%s not exist, use default configuration", configFile)
		genDefaultCfg(cfg)
	} else if err = yaml.Unmarshal(jCliBytes, cfg); err != nil {
		qLog.Errorf("Unmarshal yaml failed! %v", err)
		return
	}
	service := GetServiceName()
	if cfg.Disabled { // Jaeger Disabled
		qLog.Infof("[%s] Tracing Disabled!", service)
		return
	}
	tracer, closer, err = cfg.New(service, config.Logger(jaeger.StdLogger))
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
		return
	}
	contextMap = make(map[uint64]*stack)
	mapLock = &sync.RWMutex{}
	isTracing = true
	opentracing.SetGlobalTracer(tracer)
	qLog.Infof("[%s] Tracing Init Success! %s", service, cfgToString(cfg))
}

func ServiceClose() {
	if !isTracing {
		return
	}
	closer.Close()
}

func GetCallerOpName(skip int) (opName string) {
	filename, line, funcname := "???", 0, "???"
	pc, filename, line, ok := runtime.Caller(skip)
	if ok {
		funcname = runtime.FuncForPC(pc).Name()
		funcname = filepath.Ext(funcname)
		funcname = strings.TrimPrefix(funcname, ".")
		filename = filepath.Base(filename)
	}
	opName = fmt.Sprintf("%s:%d:%s", filename, line, funcname)
	return
}

func HttpAPISpanStart(r *http.Request, opName string) (span *JaegerSpan) {
	spanCtx, _ := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
	span = &JaegerSpan{tracer.StartSpan(opName, ext.RPCServerOption(spanCtx))}
	qLog.Debugf("TracingHttpAPISpanStart Success! %s", opName)
	return
}

func (js *JaegerSpan) SetTags(tags map[string]interface{}) {
	for key := range tags {
		js.SetTag(key, tags[key])
	}
}

func (js *JaegerSpan) SetLogs(logs map[string]string) {
	for key := range logs {
		js.LogFields(log.String(key, logs[key]))
	}
}

func (js *JaegerSpan) ContextInit() (ctx context.Context) {
	ctx = opentracing.ContextWithSpan(context.Background(), js.Span)
	return
}

func APISpanStart(opName string) (goId uint64, span *JaegerSpan) {
	goId = CurGoroutineID()
	topCtx := TopContext(goId)
	if topCtx == nil {
		spanCtx, _ := tracer.Extract(opentracing.HTTPHeaders, new(http.Header))
		span = &JaegerSpan{opentracing.StartSpan(opName, ext.RPCServerOption(spanCtx))}
	} else {
		ospan, _ := opentracing.StartSpanFromContext(topCtx, opName)
		span = &JaegerSpan{ospan}
	}
	return
}

func APISpanStartWithCtx(opName string, ctx context.Context) (goId uint64, span *JaegerSpan) {
	goId = CurGoroutineID()
	ospan, _ := opentracing.StartSpanFromContext(ctx, opName)
	span = &JaegerSpan{ospan}
	return
}

func (js *JaegerSpan) Complete() {
	js.Span.Finish()
}

func (js *JaegerSpan) BeforeHttpRequest(req *http.Request) {
	ext.SpanKindRPCClient.Set(js.Span)
	ext.HTTPUrl.Set(js.Span, req.URL.String())
	ext.HTTPMethod.Set(js.Span, req.Method)
	js.Tracer().Inject(
		js.Context(),
		opentracing.HTTPHeaders,
		opentracing.HTTPHeadersCarrier(req.Header),
	)
}

func (js *JaegerSpan) BeforeRPCRequest(tracingOpts map[string]string) {
	ext.SpanKindRPCClient.Set(js.Span)
	js.Tracer().Inject(
		js.Context(),
		opentracing.TextMap,
		opentracing.TextMapCarrier(tracingOpts),
	)
}

func TracingRPCAPISpanStart(tracingOpts map[string]string, opName string) (span *JaegerSpan) {
	spanCtx, _ := tracer.Extract(opentracing.TextMap, opentracing.TextMapCarrier(tracingOpts))
	span = &JaegerSpan{tracer.StartSpan(opName, ext.RPCServerOption(spanCtx))}
	qLog.Debugf("TracingHttpAPISpanStart Success! %s", opName)
	return
}

func PopContext(goId uint64) {
	mapLock.RLock()
	stk, ok := contextMap[goId]
	mapLock.RUnlock()
	if !ok {
		qLog.Warnf("[POP] contextMap[%d] already gone", goId)
	}
	stk.Pop()
	qLog.Debugf("[POP] contextMap[%d].Size=%d", goId, stk.Size)
	if stk.Size == 0 {
		mapLock.Lock()
		delete(contextMap, goId)
		qLog.Debugf("[POP] delete contextMap[%d]", goId)
		mapLock.Unlock()
	}
}

func PushContext(goId uint64, ctx context.Context) {
	mapLock.RLock()
	stk, ok := contextMap[goId]
	mapLock.RUnlock()
	if !ok {
		stk = New()
		mapLock.Lock()
		contextMap[goId] = stk
		qLog.Debugf("[PUSH] add contextMap[%d]", goId)
		mapLock.Unlock()
	}
	stk.Push(ctx)
	qLog.Debugf("[PUSH] contextMap[%d].Size=%d", goId, stk.Size)
}

func TopContext(goId uint64) context.Context {
	mapLock.RLock()
	stk, ok := contextMap[goId]
	mapLock.RUnlock()
	if !ok {
		stk = New()
		mapLock.Lock()
		contextMap[goId] = stk
		qLog.Debugf("[TOP] add contextMap[%d]", goId)
		mapLock.Unlock()
	}
	if stk.Size == 0 {
		return nil
	}
	return stk.Top()
}

func IsTracingEnabled() bool {
	return isTracing
}
