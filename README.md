# grpcnacos

`grpcnacos` 是一个基于 Nacos 的 gRPC 服务注册与发现库：一个 DSN 同时描述了 Nacos 服务端、要注册的服务实例和要订阅的服务名，注册器（Registrar）把实例写进 Nacos，解析器（Resolver）把服务名解析成健康实例的地址列表交给 gRPC 的 `round_robin` 之类的负载均衡策略。

## 特性

- 🚀 基于 Nacos 的服务注册与发现，一个 module、一个包
- 🔧 gRPC 服务注册器（`Registrar`）：注册 / 注销实例
- 🔍 gRPC 命名解析器（`Builder`）：注册 `nacos://` scheme，订阅 + 轮询兜底刷新地址列表
- ⚖️ 解析出的地址携带权重、集群与自定义元数据，可直接配合 gRPC 负载均衡
- 🛠️ 灵活的 DSN 配置方式，非法取值直接报错而不是被静默忽略

## 安装

```bash
go get github.com/soyacen/grpcnacos
```

## 快速开始

### 服务注册器

```go
package main

import (
    "context"
    "log"

    "github.com/soyacen/grpcnacos"
)

func main() {
    // ip / port 是注册器必填参数：它们描述「本进程对外暴露的地址」
    registrar, err := grpcnacos.NewRegistrar(
        "nacos://localhost:8848/my-service?group=DEFAULT_GROUP&ip=127.0.0.1&port=9101")
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    if err := registrar.Register(ctx); err != nil {
        log.Fatal(err)
    }

    // 服务运行...

    if err := registrar.Deregister(ctx); err != nil {
        log.Fatal(err)
    }
}
```

### 命名解析器

```go
package main

import (
    "log"
    "time"

    _ "github.com/soyacen/grpcnacos" // 导入即注册 nacos:// scheme
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    pb "your-service-package" // 替换为你的 protobuf 生成包
)

func main() {
    // 直接使用连接串：一次调用发出
    conn, err := grpc.NewClient(
        "nacos://localhost:8848/my-service?group=DEFAULT_GROUP&namespace=public",
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    client := pb.NewYourServiceClient(conn)

    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()

    resp, err := client.YourMethod(ctx, &pb.YourRequest{})
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Response: %v", resp)
}
```

只看 DSN 时用 `grpcnacos.ParseDsn`：

```go
parsed, err := grpcnacos.ParseDsn(ctx, "resolver", "nacos://localhost:8848/my-service?group=DEFAULT_GROUP")
if err != nil {
    log.Fatal(err)
}
// parsed.ClientParam  / parsed.SubscribeParam
```

可运行示例见 [example/server](example/server) 与 [example/client](example/client)，端到端脚本见 `scripts/example-e2e.sh`。

## DSN 格式

```
nacos://[username[:password]@]host[:port]/service_name?param=value
```

- scheme 只支持 `nacos://`，其他 scheme 直接报错；单台 `host:port`，端口缺省 `8848`。
- path 就是服务名，不能为空。
- 用户名 / 密码通过 userinfo 传递（`nacos://admin:nacos@host:8848/svc`）。

### 查询参数

| 参数                   | 默认值            | 适用        | 说明                                          |
| ---------------------- | ----------------- | ----------- | --------------------------------------------- |
| `namespace`          | `public`        | 全部        | 命名空间 ID                                   |
| `group`              | `DEFAULT_GROUP` | 全部        | 服务分组                                      |
| `timeout`            | `10000`         | 全部        | Nacos 客户端超时（毫秒）                      |
| `logDir`             | SDK 默认          | 全部        | Nacos SDK 日志目录                            |
| `cacheDir`           | SDK 默认          | 全部        | Nacos SDK 本地缓存目录                        |
| `logLevel`           | SDK 默认          | 全部        | Nacos SDK 日志级别（debug/info/warn/error）   |
| `notLoadCacheAtStart` | `true`          | 全部        | 启动时不加载本地缓存                          |
| `ip`                 | 无，**必填**      | 注册器      | 实例 IP                                       |
| `port`               | 无，**必填**      | 注册器      | 实例端口                                      |
| `weight`             | `10.0`          | 注册器      | 实例权重                                      |
| `ephemeral`          | `true`          | 注册器      | 是否为临时实例                                |
| `cluster`            | 空                | 注册器      | 集群名                                        |
| `meta.*`             | 无                | 注册器      | 自定义元数据，`meta.zone=z1` → `zone=z1`    |
| `clusters`           | 空（`nil`）     | 解析器      | 只订阅这些集群，逗号分隔，空项被丢弃          |

已知参数的非法取值（非数字 `timeout`/`port`/`weight`、非布尔 `ephemeral`/`notLoadCacheAtStart`、非法端口、缺少 host / 服务名 / `ip` / `port`）都会直接返回以 `grpcnacos: ` 开头的错误，不会被静默忽略。`ParseDsn` 的 `kind` 只接受 `"registrar"` 与 `"resolver"`。

### 示例

```bash
# 注册 127.0.0.1:9101 到 my-service
nacos://localhost:8848/my-service?group=DEFAULT_GROUP&ip=127.0.0.1&port=9101

# 带认证 + SDK 参数 + 元数据
nacos://admin:nacos@192.168.1.100:8848/my-service?namespace=dev&group=MY_GROUP&timeout=5000&weight=5.0&ephemeral=true&cluster=DEFAULT&meta.version=v1.0.0&logDir=/tmp/nacos/log&cacheDir=/tmp/nacos/cache&logLevel=error&notLoadCacheAtStart=true

# 解析器：只订阅 c1、c2 两个集群
nacos://localhost:8848/my-service?group=DEFAULT_GROUP&clusters=c1,c2
```

## API

```go
// 注册器
type Registrar interface {
    Register(ctx context.Context) error
    Deregister(ctx context.Context) error
}
func NewRegistrar(dsn string) (Registrar, error)

// 工厂注册表：init() 已把 "nacos" 注册进去，框架可按名字取工厂
type Factory interface {
    New(ctx context.Context, dsn string) (Registrar, error)
}
func Register(name string, resource Factory)
func Get(name string) (Factory, bool)

// DSN
type DSN struct {
    ClientParam     vo.NacosClientParam
    RegisterParam   vo.RegisterInstanceParam
    DeregisterParam vo.DeregisterInstanceParam
    SubscribeParam  vo.SubscribeParam
}
type DsnParser func(ctx context.Context, kind string, dsn string) (*DSN, error)
var DefaultDsnParser DsnParser = ParseDsn
func ParseDsn(ctx context.Context, kind string, dsn string) (*DSN, error)

// 解析器：实现 google.golang.org/grpc/resolver.Builder，Scheme() == "nacos"
type Builder struct{}
```

## 解析行为

- 解析出的地址为 `host:port`，只保留 `Healthy && Enable` 的实例，并按 `Addr` 升序排序，保证同一份实例集合产生同一份状态。
- 每个地址的 `Attributes` 带 `ServiceName`、`weight`、`cluster`、`ephemeral` 以及实例的全部自定义元数据。
- 订阅（`Subscribe`）负责实时推送，另有 5 秒周期的轮询兜底；收到推送后轮询计时器重置。
- 服务没有实例时上报空地址列表，而不是报错：gRPC 会按自己的策略进入 `TRANSIENT_FAILURE`，实例出现后自动恢复。
- `Resolver.Close` 幂等：停止轮询、退订、关闭客户端，重复调用不会 panic。
- 构建失败（DSN 非法、订阅失败、首次拉取失败）时不泄漏 Nacos 客户端。

## 开发

```bash
make all                 # fmt-check + vet + test
make test                # 单元测试
make race                # 单元测试（-race）
make lint                # fmt-check + vet + golangci-lint
make tidy                # go mod tidy

# 集成测试：需要可用的 Nacos，未设置环境变量则自动跳过
GRPCNACOS_TEST_ADDR=127.0.0.1:8848 make integration-test
```

### 用 Docker 起一个本地 Nacos

仓库自带的 `docker-compose.yml` 会启动一个单机 Nacos（`nacos/nacos-server:v2.5.2`，与 CI 同版本，关闭鉴权，数据不落盘），需要本机 Docker 已启动：

| 命令                            | 作用                                                                      |
| ------------------------------- | ------------------------------------------------------------------------- |
| `make nacos-up`               | 后台启动 Nacos（容器名`grpcnacos-nacos`，默认映射 8848/9848）          |
| `make nacos-wait`             | 轮询就绪探针，最多等 120 秒                                               |
| `make integration-test-local` | 起 Nacos → 等就绪 → 跑集成测试 → 无论成败都清理容器                  |
| `make example-e2e`            | 实际运行`example/server` 与 `example/client`，验证注册、轮询与注销传播 |
| `make verify-local`           | 上面两步合起来：一次命令完成集成测试 + 示例端到端，结束后自动清理         |
| `make nacos-down`             | 停掉容器并删除其中的数据                                                  |

```bash
make verify-local        # 推荐：一条命令完成真实 Nacos 验证
make nacos-up            # 或者手动控制生命周期
make example-e2e
make nacos-down
```

默认端口是 8848/9848，可以整体挪开（Nacos SDK 用 HTTP 端口 +1000 推导 gRPC，所以 gRPC 端口会跟着走，集成测试地址也自动跟随）：

```bash
make verify-local NACOS_HTTP_PORT=18848
```

集成测试支持的环境变量：

| 变量                        | 必填 | 说明                                                    |
| --------------------------- | ---- | ------------------------------------------------------- |
| `GRPCNACOS_TEST_ADDR`     | 是   | Nacos 地址，格式`host:port`；未设置则跳过全部集成测试 |
| `GRPCNACOS_TEST_USERNAME` | 否   | 开启鉴权时的用户名                                      |
| `GRPCNACOS_TEST_PASSWORD` | 否   | 开启鉴权时的密码                                        |

## 已知限制

- 依赖的 nacos-sdk-go v2.3.5 在 `RpcClient.Shutdown` 中删除全局 client map 时没有加锁，而 `CreateClient` 读取该 map 时加锁，因此**在 `-race` 构建下**，「注册/订阅期间关闭客户端」可能报告一条发生在 SDK 内部的 data race，堆栈落在 `nacos-sdk-go` 的 `RpcClient.Shutdown` 与 `CreateClient` 上。这是上游缺陷，本库无法在自身代码里消除；单元测试仍然全程开启 `-race`，集成测试与 CI 的集成 job 因此不启用 `-race`。
- 只支持单台 Nacos 服务端地址，不支持多地址集群列表。
- 注册器不提供 `Close`：`Deregister` 只负责摘除实例，Nacos 客户端的生命周期由调用方按进程生命周期管理。

## 依赖

- [nacos-sdk-go/v2](https://github.com/nacos-group/nacos-sdk-go) - Nacos Go SDK
- [grpc-go](https://github.com/grpc/grpc-go) - gRPC Go 实现

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

## 致谢

感谢以下开源项目：

- [Nacos](https://nacos.io/) - 动态服务发现、配置和服务管理平台
- [gRPC](https://grpc.io/) - Google 的高性能 RPC 框架
