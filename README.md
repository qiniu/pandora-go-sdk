# Pandora SDK

[![Build Status](https://travis-ci.org/qiniu/pandora-go-sdk.svg?branch=master)](https://travis-ci.org/qiniu/pandora-go-sdk) [![GoDoc](https://godoc.org/github.com/qiniu/pandora-go-sdk?status.svg)](https://godoc.org/github.com/qiniu/pandora-go-sdk)

[![Qiniu Logo](http://open.qiniudn.com/logo.png)](http://www.qiniu.com/)

# Pandora SDK使用说明

## 简述

Pandora SDK是pandora服务一个golang版本的SDK。包含pipeline、tsdb和logdb三种服务的SDK。

## 快速开始

要使用pandora SDK，首先你得有一对七牛官网上申请的并经过实名认证的AK/SK，同时拿到pandora SDK的源码，然后就可以开启pandora的大数据之旅。步骤如下：

1. 在官网获得经过身份认证的AK/SK；
2. 通过`go get github.com/qiniu/pandora-go-sdk`拿到pandora SDK的源码；
3. 将代码路径加到`GOPATH`里;
4. 参照pandora sdk使用说明开始编写代码。

这里我们给出一个小的代码片段，通过这段代码我们展示一下如何快速的用pandora SDK做第一次请求。

```go
package main

import (
    "log"
    sdk "github.com/qiniu/pandora-go-sdk/pipeline"
    sdkbase "github.com/qiniu/pandora-go-sdk/base"
)

func main() {
    ak := "AK"// 替换成自己的AK/SK
    sk := "SK"

    // 生成配置文件
    cfg := sdk.NewConfig().
        WithAccessKeySecretKey(ak, sk).
        WithEndpoint("https://pipeline.qiniu.com").
        WithLogger(sdkbase.NewDefaultLogger()).
        WithLoggerLevel(sdkbase.LogDebug)
        
    // 生成client实例
    client, err := sdk.New(cfg)
    if err != nil {
        log.Println(err)
        return
    }

    schema := []sdk.RepoSchemaEntry{sdk.RepoSchemaEntry{Key: "testKey", ValueType: "string", Required: true}}
    err = client.CreateRepo(&sdk.CreateRepoInput{RepoName: "testRepo", Region: "nb", Schema: schema}) // 创建repo
    if err != nil {
        log.Println(err)
        return
    }
    repos, err := client.ListRepos(&sdk.ListReposInput{}) // 列举repo
    if err != nil {
        log.Println(err)
        return
    }
    log.Println(repos)
}
```
上面是一个简单但是可以运行的代码片段，展示了一个最简单的场景，接下来我们将逐步的介绍如何借助SDK本身的一些基础设置更好的使用。

## pandora sdk提供的基础设施

### 签名

Pandora SDK封装了签名逻辑，用户无需手动计算签名，避免大量重复无意义的工作。签名的方式分为以下两种：

```
1. AK/SK直接签名，用户只要设置好AK/SK之后用SDK访问即可，这种方式对每一个请求都会计算一次签名；
2. token签名。用户的应用服务器签好一个token，在token中设定一个过期时间（超过过期时间之后token就失效了，需要重新签发，这也是对安全性的保证），然后分发给自己的app（例如手机端、摄像头之类的），让app拿着这个token直接访问。好处是app不需要保存AK/SK，安全性得到了比较大的提高。
```
由于token签名方式较为复杂，因此这里做一些详述。

首先，为什么要有token，其实这是一种保护用户主AK/SK的方式，假如用户有1000台设备，或者在某个手机app上要向pipeline打点，但是把自己的AK/SK分发出来的风险是极高的，因此设置过期时间之后签发一批token，设备或者手机app拿着签发出来的token在过期时间之内都可以访问pipeline，既能保护AK/SK，也可以防止token一直被使用而泄露权限。

我们要使用token，必须借助于一个定义于token.go里面的辅助结构体TokenDesc，它的定义如下：

```gp
type TokenDesc struct {
        Url         string
        QueryString url.Values
        Expires     int64
        ContentMD5  string
        ContentType string
        Method      string
        Headers     http.Header
}
```

了解了这个重要的数据结构，接下来我们来看一段代码示例：

```go
desc := &TokenDesc{}
desc.Expires = time.Now().Unix() + 3600 // token过期时间为1小时之后

// 下面这三行可以看出来这是对打点接口做授权，支持向repo_post_data这个repo打点
desc.Method = "POST"
desc.Url = "/v2/repos/repo_post_data/data"
desc.ContentType = "application/text"

token := client.MakeToken(td) // 通过MakeToken真正的生成一个token字符串


postDataInput := &PostDataInput{
    RepoName: "repo_post_data_with_token",
    Points: Points{
        Point{
            []PointField{
                PointField{
                    Key:   "f1",
                    Value: 12.7,
                },
            },
        },
    },
    // 这是上一步签发出的token
    PipelineToken: PipelineToken{
        Token: token,
    },
}
// 注意，这里是client2用这个token在访问
err = client2.PostData(postDataInput)
if err != nil {
    log.Println(err)
}
```
上面的代码里面我们清楚地看到由client签发出一个token，然后交给client2来使用，token在签发的时候设置了一些条件，比如过期时间用来限定token何时过期，而其他的字段如method、url等等是对这个请求本身的一些描述。

### Error

Pandora SDK中封装了RequestError，来表示服务端返回的错误，方便用户快速得到出错的详细信息。
RequestError的定义如下：

```go
type RequestError struct {
        Message    string `json:"error"`
        StatusCode int    `json:"-"`
        RequestId  string `json:"-"`
        RawMessage string `json:"-"`
        ErrorType  int    `json:"-"`
}

func (r RequestError) Error() string {
        return fmt.Sprintf("pipeline: service returned error: StatusCode=%d, ErrorMessage=%s, RequestId=%s", r.StatusCode, r.Message, r.RequestId)
}
```
凡是发往服务的请求出错之后我们都能得到一个RequestError类型的错误，从定义可以知道该错误中包含了HTTP状态码、RequestId、Error message以及请求中的原始body内容。如果不幸SDK并未帮我们顺利的解析出Message，我们可以从RawMessage中取值来查看body里面究竟返回了什么。除此之外还有一个ErrorType字段，归类了一部分比较常见的错误码。

所有的ErrorType，在pipeline/tsdb/logdb目录下的error.go内定义，如下：

|错误|描述|HTTP状态码|
|:------:|:------:|:------:|
|NoSuchGroupError|访问的group不存在|404|
|GroupAlreadyExistsError|创建的group已存在|409|
|NoSuchRepoError|访问的repo不存在|404|
|RepoAlreadyExistsError|创建的repo已存在|409|
|NoSuchTransformError|访问的transform不存在|404|
|TransformAlreadyExistsError|创建的transform已存在|409|
|NoSuchExportError|访问的export不存在|404|
|ExportAlreadyExistsError|创建的export已存在|409|
|NoSuchPluginError|访问的plugin不存在|404|
|PluginAlreadyExistsError|要上传的plugin已存在|409|
|RepoInCreatingError|访问的repo正在创建中（transform创建的dest repo）|202|
|RepoCascadingError|repo上面有级联的export或者transform|409|
|InvalidTransformSpecError|transform spec非法，有可能是sql不合法、plugin非法，或者interval不合法|400|
|InvalidExportSpecError|export的spec非法|400|
|InternalServerError|系统内部错误|500|
|UnauthorizedError|请求未授权，或者授权未通过|401|

对于这些错误类型的使用举一个例子，如下：

```go
output, err := client.GetTransform(input)
if err == nil { // 没有出错，做一些处理
    // do something
    return
}
v, ok := err.(*reqerr.RequestError)
if !ok {
    // do something
}
switch v.ErrorType {
case *NoSuchRepoError:
    // do something
case *NoSuchTransformError:
    // do something
case *InternalServerError:
    // do something
case *UnauthorizedError:
    // do something
case *RequestError:
    // do something
default:
    // do something
}
```
可以看到由于提供了详尽的错误类型，所以用户可以方便的定位问题并采取对应的处理，不必去根据返回的body判断错误类型，摆脱对low level信息的判断和处理。
当然，如果用户觉得这些额外提供的错误类型太过于繁琐，那么可以把返回的错误当成普通的error来处理也没有任何问题。

### API封装

Pandora SDK提供了对所有核心API的封装，各个接口的输入类型定义在pipeline/tsdb/logdb目录下的models.go里面，结构体的定义和API的定义是接近的，具体的可以参考[Pandora产品文档](https://pandora-docs.qiniu.com/)和sample目录下的示例代码，此处不再赘述。

## 使用原则
1. 所有接口调用均遵循“没有消息就是好消息”的原则，返回的error如果是nil，那么表明调用成功；
2. 当请求出错的时候尽可能的将error转换为RequestError或者它的派生错误类型，既可以快速定位错误类型，又可以获取到RequestId，便于服务端定位问题；
3. 在client实例上调用任何接口都是天然支持并发的，无需使用同步机制。


## FAQ

1. Q: SDK提供了token签名方式，怎么使用？

   A: 每一个接口的输入，例如`CreateRepoInput`中有一个`PipelineToken`的结构，包含了一个`Token`字段，是一个字符串，当此字段为空的时候表示使用AK/SK签名，不为空的时候使用字符串的值作为token进行访问。
2. Q: 我使用SDK访问pipeline，为什么总是返回401 Unauthorized错误？

   A: 返回401 Unauthorized的情况比较复杂，由于SDK封装了签名算法不会犯低级错误，所以分两类来看：
   1)、AK/SK签名：可以看看机器时间，pandora的签名方式要求客户端时间和服务器时间相差不得超过15分钟，所以一定要确保时间没有跑偏;
   2)、token签名：首先检查签发的token里面expires是不是已经过期，过期的token会失去访问服务的权限，然后再看看token里面的url、method之类的有没有设置错误，token中的各个字段的值必须和请求中的实际情况相一致。
   3)、检查一下是否在官网上做过账号的实名认证，未经过实名认证的账号是无法正常访问的。


