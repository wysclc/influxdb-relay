# InfluxDB Relay

This project adds a basic high availability layer to InfluxDB. With the right architecture and disaster recovery processes, this achieves a highly available setup.

*NOTE:* `influxdb-relay` must be built with Go 1.13+

## Usage

To build from source and run:

```sh
$ # Install influxdb-relay to your $GOPATH/bin
$ go get -u github.com/influxdata/influxdb-relay
$ # Edit your configuration file
$ cp $GOPATH/src/github.com/influxdata/influxdb-relay/sample.toml ./relay.toml
$ vim relay.toml
$ # Start relay!
$ $GOPATH/bin/influxdb-relay -config relay.toml
```

## Configuration

```toml
[[http]]
# Name of the HTTP server, used for display purposes only.
name = "example-http"

# TCP address to bind to, for HTTP server.
bind-addr = "127.0.0.1:9096"

# 该 relay 的 output worker 共用一个持久化队列文件。
queue-path = "/var/lib/influxdb-relay/example-http.queue.db"

# Enable HTTPS requests.
ssl-combined-pem = "/etc/ssl/influxdb-relay.pem"

# Array of InfluxDB instances to use as backends for Relay.
output = [
    # name: name of the backend, used for display purposes only.
    # location: full URL of the /write endpoint of the backend
    # timeout: Go-parseable time duration. Fail writes if incomplete in this time.
    # buffer-size-mb: 单个 output 的活动持久化队列上限。
    # max-batch-kb: 单个 output worker 合并的最大请求体。
    # max-delay-interval: 最大重试间隔。
    # skip-tls-verification: skip verification for HTTPS location. WARNING: it's insecure. Don't use in production.
    { name="local1", location="http://127.0.0.1:8086/write", timeout="10s", buffer-size-mb=100, max-batch-kb=512, max-delay-interval="5s" },
    { name="local2", location="http://127.0.0.1:7086/write", timeout="10s", buffer-size-mb=100, max-batch-kb=512, max-delay-interval="5s" },
]

[[udp]]
# Name of the UDP server, used for display purposes only.
name = "example-udp"

# UDP address to bind to.
bind-addr = "127.0.0.1:9096"

# Socket buffer size for incoming connections.
read-buffer = 0 # default

# Precision to use for timestamps
precision = "n" # Can be n, u, ms, s, m, h

# Array of InfluxDB instances to use as backends for Relay.
output = [
    # name: name of the backend, used for display purposes only.
    # location: host and port of backend.
    # mtu: maximum output payload size
    { name="local1", location="127.0.0.1:8089", mtu=512 },
    { name="local2", location="127.0.0.1:7089", mtu=1024 },
]
```

## Description

The architecture is fairly simple and consists of a load balancer, two or more InfluxDB Relay processes and two or more InfluxDB processes. The load balancer should point UDP traffic and HTTP POST requests with the path `/write` to the two relays while pointing GET requests with the path `/query` to the two InfluxDB servers.

The setup should look like this:

```
        ┌─────────────────┐                 
        │writes & queries │                 
        └─────────────────┘                 
                 │                          
                 ▼                          
         ┌───────────────┐                  
         │               │                  
┌────────│ Load Balancer │─────────┐        
│        │               │         │        
│        └──────┬─┬──────┘         │        
│               │ │                │        
│               │ │                │        
│        ┌──────┘ └────────┐       │        
│        │ ┌─────────────┐ │       │┌──────┐
│        │ │/write or UDP│ │       ││/query│
│        ▼ └─────────────┘ ▼       │└──────┘
│  ┌──────────┐      ┌──────────┐  │        
│  │ InfluxDB │      │ InfluxDB │  │        
│  │ Relay    │      │ Relay    │  │        
│  └──┬────┬──┘      └────┬──┬──┘  │        
│     │    |              |  │     │        
│     |  ┌─┼──────────────┘  |     │        
│     │  │ └──────────────┐  │     │        
│     ▼  ▼                ▼  ▼     │        
│  ┌──────────┐      ┌──────────┐  │        
│  │          │      │          │  │        
└─▶│ InfluxDB │      │ InfluxDB │◀─┘        
   │          │      │          │           
   └──────────┘      └──────────┘           
 ```



The relay will listen for HTTP or UDP writes and write the data to each InfluxDB server via the HTTP write or UDP endpoint, as appropriate.

HTTP 推荐启用持久化队列。启用后，一次请求会在一个本地事务中写入所有尚有空间的 output 独立队列；至少一个 output 入队成功后 relay 返回 204，各 output worker 再异步投递。某个远端 output 故障或队列满不会阻塞其他 output，也不会让请求 goroutine 等待远端恢复。未配置持久化队列时仍保留旧的同步转发行为，仅用于兼容旧配置。

With this setup a failure of one Relay or one InfluxDB can be sustained while still taking writes and serving queries. However, the recovery process might require operator intervention.

## 持久化队列

为同一个 HTTP relay 的**全部** output 配置正数 `buffer-size-mb` 后启用。不能在一个 relay 内混用持久化和同步 output，因为两种模式的 ACK 和投递语义不同。

配置项：

* `queue-path`：该 HTTP relay 的 BoltDB 队列文件。默认位于 `/var/lib/influxdb-relay`。
* `buffer-size-mb`：单个 output 活动队列的逻辑容量上限。
* `max-batch-kb`：worker 一次合并投递的最大请求体，默认 512KB。
* `max-delay-interval`：指数退避上限，默认 10 秒；实际等待会加入随机抖动。
* `timeout`：单次远端 HTTP 请求超时，默认 10 秒。

通过解析和校验的请求在持久化阶段按以下规则响应：

1. 至少一个 output 成功入队时返回 204。已满 output 会静默跳过本次记录，避免队列容量日志淹没真正的 HTTP 错误。
2. 所有 output 都满时返回 503 和 `Retry-After: 1`。
3. WAL 事务提交失败时，所有本次入队写入都会回滚并返回 503。

跳过满队列可以保证正常 output 持续接收，但满队列 output 会永久缺失这部分数据。如果业务要求每个 output 一条不漏，应通过外部磁盘和队列监控提前扩容，而不能只依赖客户端 204 或 relay 日志。

每个 output 严格从自己的 FIFO 队列头部消费。网络错误、408、425、429 和 5xx 使用指数退避重试；其他响应进入该 output 的 dead-letter，避免错误数据永久堵住队首。dead-letter 的逻辑容量也以该 output 的 `buffer-size-mb` 为上限，超过后淘汰最旧记录并记录日志。

对于已经入队的 output，投递保证是 **at-least-once**：relay 在远端写成功之后、删除 WAL 记录之前崩溃时，重启后会重放该记录。规范化后的点包含固定时间戳，因此重放相同 series/timestamp 通常由 InfluxDB 按覆盖写处理，但业务仍不应假设 exactly-once。

容量规划至少应预留 `2 × 所有 output 的 buffer-size-mb 之和`，再加 BoltDB 页和文件系统开销。BoltDB 会复用已释放页面，但队列清空后文件大小不会立即缩小。队列文件以 `0600` 创建，其中包含请求正文、查询参数和 Authorization header；请限制目录权限并纳入磁盘监控。output 的 `name` 是持久化 bucket 标识，存在积压时不要随意改名。

## Recovery

InfluxDB organizes its data on disk into logical blocks of time called shards. We can use this to create a hot recovery process with zero downtime.

The length of time that shards represent in InfluxDB are typically 1 hour, 1 day, or 7 days, depending on the retention duration, but can be explicitly set when creating the retention policy. For the sake of our example, let's assume shard durations of 1 day.

Let's say one of the InfluxDB servers goes down for an hour on 2016-03-10. Once midnight UTC rolls over, all InfluxDB processes are now writing data to the shard for 2016-03-11 and the file(s) for 2016-03-10 have gone cold for writes. We can then restore things using these steps:

1. Tell the load balancer to stop sending query traffic to the server that was down (this should be done as soon as an outage is detected to prevent partial or inconsistent query returns.)
2. Create backup of 2016-03-10 shard from a server that was up the entire day
3. Restore the backup of the shard from the good server to the server that had downtime
4. Tell the load balancer to resume sending queries to the previously downed server

During this entire process the Relays should be sending current writes to all servers, including the one with downtime.

## Sharding

It's possible to add another layer on top of this kind of setup to shard data. Depending on your needs you could shard on the measurement name or a specific tag like `customer_id`. The sharding layer would have to service both queries and writes.

As this relay does not handle queries, it will not implement any sharding logic. Any sharding would have to be done externally to the relay.


## Caveats

While `influxdb-relay` does provide some level of high availability, there are a few scenarios that need to be accounted for:

- `influxdb-relay` will not relay the `/query` endpoint, and this includes schema modification (create database, `DROP`s, etc). This means that databases must be created before points are written to the backends.
- Continuous queries will still only write their results locally. If a server goes down, the continuous query will have to be backfilled after the data has been recovered for that instance.
- 每个 output 内部保持 FIFO，但故障 output 清空积压前仍落后于正常 output。查询负载均衡器应在该 output 追平前将其摘除，避免读到不完整数据。
- WAL 提供 at-least-once 而不是 exactly-once。重放相同 series/timestamp 时可能覆盖字段，因此应避免依赖写入次数产生业务副作用。

## Building

The recommended method for building `influxdb-relay` is to use Docker
and the included `Dockerfile_build_ubuntu64` Dockerfile, which
includes all of the necessary dependencies.

To build the docker image, you can run:

```
docker build -f Dockerfile_build_ubuntu64 -t influxdb-relay-builder:latest .
```

And then to build the project:

```
docker run --rm -v $(pwd):/root/go/src/github.com/influxdata/influxdb-relay influxdb-relay-builder
```

*NOTE* By default, builds will be for AMD64 Linux (since the container
is running AMD64 Linux), but to change the target platform or
architecture, use the `--platform` and `--arch` CLI options.

Which should immediately call the included `build.py` build script,
and leave any build output in the `./build` directory. To see a list
of available build commands, append a `--help` to the command above.

```
docker run -v $(pwd):/root/go/src/github.com/influxdata/influxdb-relay influxdb-relay-builder --help
```

### Packages

To build system packages for Linux (`deb`, `rpm`, etc), use the
`--package` option:

```
docker run -v $(pwd):/root/go/src/github.com/influxdata/influxdb-relay influxdb-relay-builder --package
```

To build packages for other platforms or architectures, use the
`--platform` and `--arch` options. For example, to build an amd64
package for Mac OS X, use the options `--package --platform darwin`.
