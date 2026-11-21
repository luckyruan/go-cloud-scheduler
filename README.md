# Go 云原生分布式任务调度平台

2026-10-04 已补有限重试、领取 token、长任务续租和下游幂等键；5 个顶层测试、vet、build 通过，真实 PostgreSQL 测试跳过，未启动 Docker。已有数据库需要执行新增迁移并同步升级 Worker，详见 [完善记录与验收](docs/06-完善记录与验收.md)。

一个面向实习简历的、可运行且可解释的 Go 后端项目。它把“定时任务”拆成调度控制面与无状态 Worker：控制面计算触发时间并持久化执行实例，多个 Worker 通过数据库行锁安全竞争任务，支持失败重试、超时接管、心跳、指标与容器化部署。

## 技术栈

- Go 1.23、标准库 `net/http`
- PostgreSQL 17、`FOR UPDATE SKIP LOCKED`
- Prometheus 指标
- Docker Compose、Kubernetes
- `robfig/cron`、`pgx`

## 快速启动

```bash
docker compose up -d --build
curl http://localhost:8090/healthz
```

创建每分钟执行一次的 HTTP 任务：

```bash
curl -X POST http://localhost:8090/api/v1/tasks \
  -H "X-API-Key: local-scheduler-key" \
  -H "Content-Type: application/json" \
  -d '{"name":"health-check","cronExpression":"*/1 * * * *","payload":{"url":"http://app:8090/healthz","method":"GET"},"timeoutSeconds":10,"maxRetries":3,"enabled":true}'
```

也可以对返回的任务 ID 调用 `POST /api/v1/tasks/{id}/trigger` 立即触发。查看指标：`GET /metrics`。

停止环境：

```bash
docker compose down
```

## 核心接口

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/v1/tasks` | 创建 Cron 任务 |
| GET | `/api/v1/tasks` | 查询任务 |
| POST | `/api/v1/tasks/{id}/trigger` | 手动触发 |
| POST | `/api/v1/executions/claim?workerId=x` | Worker 抢占任务 |
| POST | `/api/v1/executions/{id}/complete` | 上报结果 |
| POST | `/api/v1/executions/{id}/heartbeat` | 执行续租，必须携带 workerId 和 claimToken |
| POST | `/api/v1/workers/heartbeat` | Worker 心跳 |
| GET | `/metrics` | Prometheus 指标 |

详细设计见[架构文档](docs/01-项目介绍与架构.md)和[完善记录与验收](docs/06-完善记录与验收.md)。
