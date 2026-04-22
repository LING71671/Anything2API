# 多平台与新平台接入开发指南

> 本文档根据当前代码结构整理，用于指导 sub2api 后续接入更多模型平台、兼容渠道或特殊上游。

如果你希望按“人类尽量少做事，AI 负责调研、设计、编码和测试”的方式推进，请先看：

- [新平台接入：人类最少工作指南与 AI 提示词](PLATFORM_ONBOARDING_MINIMAL_HUMAN_GUIDE_CN.md)

## 一、先判断：是否真的需要新增平台

项目里的“平台”不是简单展示字段，而是贯穿分组、账号、调度、请求解析、上游转发、计费、前端管理的核心维度。新增平台前，先按下面规则判断。

| 场景 | 推荐做法 | 原因 |
|------|----------|------|
| 新上游兼容 Anthropic Messages API | 复用 `anthropic` 平台，新增 `apikey`/`upstream` 账号，配置 `base_url`、`api_key`、`model_mapping` | 成本最低，可直接复用 Claude 请求解析、流式处理、计费和调度 |
| 新上游兼容 OpenAI Responses/Chat Completions | 复用 `openai` 平台，配置 API Key 或 OAuth 账号 | 可复用 `OpenAIGatewayService` 的请求转换、WS、Responses 逻辑 |
| 新上游兼容 Gemini Native API | 复用 `gemini` 平台，配置对应账号 | 可复用 `/v1beta/models` Gemini 原生兼容层 |
| 新上游只是在某个平台下的特殊鉴权或签名 | 优先新增账号类型或账号 `credentials` 配置 | 例如 Bedrock 是 `anthropic` 平台下的 `bedrock` 账号类型，而不是独立平台 |
| 新上游协议、鉴权、响应格式、错误语义、用量统计都不同 | 新增平台 | 需要独立 gateway service、路由、用量解析、前端表单和测试 |

当前项目里的 Bedrock 是一个重要参考：它并没有新增 `bedrock` 平台，而是作为 `anthropic` 平台下的 `AccountTypeBedrock` 存在。它复用了 Anthropic 入站协议，只在上游请求构造、SigV4/API Key 鉴权、模型 ID 解析、响应处理上做特殊分支。

## 二、当前平台能力地图

| 平台 | 常量值 | 主要入口 | 说明 |
|------|--------|----------|------|
| Anthropic | `anthropic` | `GatewayHandler` + `GatewayService` | Claude Messages 主链路，支持 OAuth、Setup Token、API Key、Bedrock |
| OpenAI | `openai` | `OpenAIGatewayHandler` + `OpenAIGatewayService` | OpenAI Responses、Chat Completions、Messages 调度、WS 模式 |
| Gemini | `gemini` | `GatewayHandler` + `GeminiMessagesCompatService` | Gemini Native API `/v1beta/models`，也支持 Claude 兼容入口调度 Gemini |
| Antigravity | `antigravity` | `AntigravityGatewayService` | 同时承载 Claude/Gemini 风格请求，支持混合调度配置 |

平台值会存储在：

- `accounts.platform`
- `groups.platform`
- `channel_model_pricing.platform`
- `usage_logs`、ops、dashboard 等统计字段

因此新增平台必须保持这些层一致。

## 三、核心代码地图

### 后端常量与模型

| 文件 | 作用 |
|------|------|
| `backend/internal/domain/constants.go` | 平台、账号类型、状态等底层常量 |
| `backend/internal/service/domain_constants.go` | service 层重新导出 domain 常量 |
| `backend/internal/service/account.go` | `Account` 结构、账号类型判断、模型映射、base URL、配额/池模式能力 |
| `backend/internal/service/group.go` | `Group` 结构、平台关联、分组级调度配置 |
| `backend/internal/service/channel.go` | Channel、定价、模型映射结构 |
| `backend/internal/service/channel_service.go` | Channel 平台隔离、模型映射、定价解析 |

### 后端管理 API

| 文件 | 作用 |
|------|------|
| `backend/internal/handler/admin/account_handler.go` | 账号创建/编辑/测试/刷新等管理接口 |
| `backend/internal/handler/admin/group_handler.go` | 分组创建/编辑，当前平台 `oneof` 校验在这里 |
| `backend/internal/handler/admin/channel_handler.go` | 渠道管理、平台定价配置 |
| `backend/internal/service/admin_service.go` | 创建账号/分组、绑定分组、混合渠道风险、隐私设置等业务逻辑 |

### 后端网关入口

| 文件 | 作用 |
|------|------|
| `backend/internal/server/routes/gateway.go` | 网关 HTTP 路由注册，按分组平台分流 |
| `backend/internal/handler/endpoint.go` | 入站 endpoint 标准化和上游 endpoint 推导 |
| `backend/internal/handler/gateway_handler.go` | Claude/Gemini/Antigravity 兼容入口主 handler |
| `backend/internal/handler/openai_gateway_handler.go` | OpenAI 专用 handler |
| `backend/internal/service/gateway_request.go` | 入站请求体解析，当前区分 Gemini 原生格式和默认 Messages 格式 |

### 后端平台转发实现

| 文件 | 作用 |
|------|------|
| `backend/internal/service/gateway_service.go` | Anthropic 主链路、调度、并发、failover、usage、Bedrock 分支 |
| `backend/internal/service/openai_gateway_service.go` | OpenAI 上游转发、Responses/Chat/WS、OpenAI usage |
| `backend/internal/service/gemini_messages_compat_service.go` | Gemini 原生和兼容转发 |
| `backend/internal/service/antigravity_gateway_service.go` | Antigravity Claude/Gemini 转发、模型映射、协议转换 |
| `backend/internal/pkg/openai` | OpenAI 协议细节 |
| `backend/internal/pkg/gemini` | Gemini 模型/协议辅助 |
| `backend/internal/pkg/antigravity` | Antigravity 协议转换 |

### 前端管理页面

| 文件 | 作用 |
|------|------|
| `frontend/src/types/index.ts` | `GroupPlatform`、`AccountPlatform` 联合类型 |
| `frontend/src/utils/platformColors.ts` | 平台颜色和样式 |
| `frontend/src/components/common/PlatformIcon.vue` | 平台图标 |
| `frontend/src/components/common/PlatformTypeBadge.vue` | 平台/账号类型徽标 |
| `frontend/src/components/admin/account/AccountTableFilters.vue` | 账号列表平台筛选 |
| `frontend/src/components/account/CreateAccountModal.vue` | 创建账号表单，平台和账号类型入口 |
| `frontend/src/components/account/EditAccountModal.vue` | 编辑账号表单 |
| `frontend/src/views/admin/GroupsView.vue` | 分组平台选择和平台特定配置 |
| `frontend/src/views/admin/ChannelsView.vue` | 渠道平台模型映射和定价 |
| `frontend/src/i18n/locales/zh.ts`、`en.ts` | 平台文案 |

## 四、新平台接入推荐流程

### 第 1 步：确定平台协议类型

先写一页简短设计记录，回答这些问题：

| 问题 | 需要确认的内容 |
|------|----------------|
| 入站协议 | 用户请求是 Claude Messages、OpenAI Responses、OpenAI Chat Completions、Gemini Native，还是新协议 |
| 上游协议 | 上游 API 的 URL、请求体、流式格式、错误格式、usage 格式 |
| 鉴权方式 | API Key、OAuth、JWT、签名、临时 token、是否需要刷新 |
| 模型映射 | 是否允许任意模型，是否需要默认映射，是否支持通配符 |
| 计费 | usage 是否有 input/output/cache/image/reasoning 等字段 |
| 限流 | 429/401/403/5xx 分别如何处理，是否需要临时不可调度 |
| 前端配置 | 创建账号时需要哪些 credentials/extra 字段 |

如果答案显示新平台只是“兼容某个现有协议”，优先复用现有平台。

### 第 2 步：新增平台常量

修改：

- `backend/internal/domain/constants.go`
- `backend/internal/service/domain_constants.go`

示例：

```go
const (
    PlatformAnthropic   = "anthropic"
    PlatformOpenAI      = "openai"
    PlatformGemini      = "gemini"
    PlatformAntigravity = "antigravity"
    PlatformFoo         = "foo"
)
```

注意：平台值一旦进入数据库，后续不要随意改名。需要改名时应写 migration 做数据迁移。

### 第 3 步：放开管理 API 校验

分组创建/更新目前限制平台枚举：

- `backend/internal/handler/admin/group_handler.go`

需要把 `binding:"omitempty,oneof=anthropic openai gemini antigravity"` 扩展为包含新平台。

账号创建请求的 `platform` 字段没有 `oneof`，但仍要检查这些逻辑：

- `backend/internal/service/admin_service.go` 的默认分组绑定
- 分组和账号绑定时的平台一致性
- `RequireOAuthOnly`、`RequirePrivacySet` 这类平台特定规则
- 混合渠道风险检查，当前主要面向 Anthropic/Antigravity

### 第 4 步：决定是否新增账号类型

如果只是同一平台下的新鉴权方式，优先新增 `AccountType`，而不是新增平台。

常量位置：

- `backend/internal/domain/constants.go`
- `backend/internal/service/domain_constants.go`

前端类型位置：

- `frontend/src/types/index.ts`

新增账号类型时，还要补：

- 创建/编辑账号表单
- `Account.IsAPIKeyOrBedrock()` 这类能力判断
- 账号测试逻辑
- 配额、池模式、用量展示是否适用

## 五、后端网关接入细节

### 1. 路由分流

网关路由集中在 `backend/internal/server/routes/gateway.go`。

当前逻辑大致是：

- `/v1/messages`：OpenAI 分组走 `h.OpenAIGateway.Messages`，其他走 `h.Gateway.Messages`
- `/v1/responses`：OpenAI 分组走 `h.OpenAIGateway.Responses`，其他走 `h.Gateway.Responses`
- `/v1/chat/completions`：OpenAI 分组走 `h.OpenAIGateway.ChatCompletions`，其他走 Claude 兼容逻辑
- `/v1beta/models`：Gemini 原生入口
- `/antigravity/...`：强制平台为 Antigravity

新增平台时有三种做法：

| 做法 | 适用场景 |
|------|----------|
| 复用 `h.Gateway.*` | 新平台入站协议和 Anthropic/Gemini 兼容，只有上游分支不同 |
| 新增 `FooGatewayHandler` | 新平台有独立入站 endpoint 或错误响应格式 |
| 在现有 handler 中加平台分支 | 新平台和现有协议高度相似，只需局部分流 |

### 2. Endpoint 标准化

修改 `backend/internal/handler/endpoint.go`。

需要补：

- 新入站路径常量
- `NormalizeInboundEndpoint`
- `DeriveUpstreamEndpoint`
- 对应测试 `endpoint_test.go`

usage 和 ops 里会记录 inbound/upstream endpoint，新增平台不要绕过这层。

### 3. 请求解析

入口是 `backend/internal/service/gateway_request.go` 的 `ParseGatewayRequest`。

当前支持：

- Gemini 原生：`systemInstruction.parts`、`contents`
- 默认 Messages/OpenAI 风格：`system`、`messages`

如果新平台入站 body 不同，可以：

- 给 `ParseGatewayRequest` 增加 `case domain.PlatformFoo`
- 或新增 `ParseFooGatewayRequest`
- 或在专用 handler 内独立解析

建议保持 `ParsedRequest` 里至少有：

- `Body`
- `Model`
- `Stream`
- `Messages`
- `System`
- `MetadataUserID`
- `GroupID`

这些字段会影响调度、粘性会话、计费、审计和兼容逻辑。

### 4. 调度接入

调度核心在 `backend/internal/service/gateway_service.go`。

重点关注：

- `resolvePlatform`
- `listSchedulableAccounts`
- `isAccountAllowedForPlatform`
- `SelectAccountWithLoadAwareness`
- `selectAccountForModelWithPlatform`
- `isModelSupportedByAccountWithContext`

新增平台默认应该严格隔离：分组平台为 `foo` 时，只调度 `accounts.platform = foo` 的账号。

如果要允许跨平台混合调度，一定要显式开关，不能默认混合。Antigravity 的混合调度就是特殊案例，逻辑复杂且有签名/上下文风险。

### 5. 新增 GatewayService

如果新平台是完整独立协议，建议新增：

- `backend/internal/service/foo_gateway_service.go`
- `backend/internal/service/foo_gateway_service_test.go`

一个最小服务至少要覆盖：

```go
type FooGatewayService struct {
    accountRepo AccountRepository
    httpClient  *http.Client
    cfg         *config.Config
}

func NewFooGatewayService(...) *FooGatewayService {
    return &FooGatewayService{...}
}

func (s *FooGatewayService) Forward(
    ctx context.Context,
    c *gin.Context,
    account *Account,
    body []byte,
) (*ForwardResult, error) {
    // 1. 提取原始模型、stream
    // 2. 应用账号/渠道模型映射
    // 3. 构造上游请求
    // 4. 发送请求
    // 5. 处理非 2xx 错误和 failover
    // 6. 处理流式/非流式响应
    // 7. 提取 usage，返回 ForwardResult
}
```

如果新平台返回格式和现有 `ForwardResult` 不兼容，优先扩展统一结构，而不是让 handler 绕过 usage 记录。

### 6. 上游鉴权

账号凭证一般放在 `Account.Credentials`：

```json
{
  "api_key": "...",
  "base_url": "https://api.foo.example.com",
  "model_mapping": {
    "claude-sonnet-4-5": "foo-large"
  }
}
```

运行态、展示态或非敏感状态一般放在 `Account.Extra`：

```json
{
  "tier_id": "pro",
  "privacy_mode": "privacy_set",
  "quota_reset_at": 1770000000
}
```

敏感字段要确认 repository 层是否会加密。新增凭证字段时，检查账号导入/导出、脱敏、前端展示是否会泄露。

### 7. 错误、限流和临时不可调度

新增平台需要定义：

| 状态 | 推荐行为 |
|------|----------|
| 401 | OAuth/token 类账号通常需要刷新或标记错误；API Key 类账号可标记不可用 |
| 403 | 判断是否余额不足、权限不足、模型不可用 |
| 429 | 解析 reset 时间，设置 rate limit 或 temp unschedulable |
| 5xx | 触发 failover，必要时记录 overload cooldown |
| 流式中断 | 不可盲目 failover，避免把两个上游流拼到一个客户端响应 |

相关文件：

- `backend/internal/service/ratelimit_service.go`
- `backend/internal/handler/failover_loop.go`
- `backend/internal/service/temp_unsched.go`
- `backend/internal/service/overload_cooldown_test.go`

## 六、模型映射、渠道和计费

### 1. 账号级模型映射

账号模型映射在 `credentials.model_mapping`。

实现位置：

- `backend/internal/service/account.go`

能力：

- 精确匹配
- 后缀 `*` 通配符
- 最长匹配优先
- 无映射时默认透传
- Antigravity 有默认映射
- Bedrock 有独立默认映射逻辑

新平台如果需要默认映射，可以新增：

```go
var DefaultFooModelMapping = map[string]string{
    "claude-sonnet-4-5": "foo-sonnet",
    "gpt-5.4": "foo-gpt",
}
```

然后在 `Account.resolveModelMapping` 中按平台返回默认映射。

### 2. 渠道级映射和定价

渠道逻辑在 `backend/internal/service/channel_service.go`。

当前设计是平台严格隔离：

```go
func isPlatformPricingMatch(groupPlatform, pricingPlatform string) bool {
    return groupPlatform == pricingPlatform
}
```

这意味着：

- `anthropic` 分组只吃 `anthropic` 定价
- `openai` 分组只吃 `openai` 定价
- 新增 `foo` 后，也应该只吃 `foo` 定价

新增平台后，需要确保前端渠道配置能选择该平台，并能为该平台配置模型映射/定价。

### 3. Usage 记录

每次转发最终应记录：

- 请求模型 `requested_model`
- 上游模型 `upstream_model`
- 计费模型
- input/output/cache/image tokens
- inbound endpoint
- upstream endpoint
- channel mapping chain
- account/group/platform

相关逻辑分散在：

- `GatewayService` usage 记录
- `OpenAIGatewayService` usage 记录
- `AntigravityGatewayService` usage 记录
- `backend/internal/service/usage_log.go`
- `backend/internal/repository/usage_log_repo.go`

新增平台不要只把响应写回客户端，而忘记 usage，否则账单、限额、dashboard、ops 都会缺数据。

## 七、前端接入清单

### 1. 类型定义

修改：

- `frontend/src/types/index.ts`

示例：

```ts
export type GroupPlatform = 'anthropic' | 'openai' | 'gemini' | 'antigravity' | 'foo'
export type AccountPlatform = 'anthropic' | 'openai' | 'gemini' | 'antigravity' | 'foo'
```

如果新增账号类型：

```ts
export type AccountType = 'oauth' | 'setup-token' | 'apikey' | 'upstream' | 'bedrock' | 'foo-token'
```

### 2. 平台样式和图标

修改：

- `frontend/src/utils/platformColors.ts`
- `frontend/src/components/common/PlatformIcon.vue`
- `frontend/src/components/common/PlatformTypeBadge.vue`

即使暂时没有专属 SVG，也要保证 fallback 显示正常。

### 3. 账号管理

重点修改：

- `frontend/src/components/admin/account/AccountTableFilters.vue`
- `frontend/src/components/account/CreateAccountModal.vue`
- `frontend/src/components/account/EditAccountModal.vue`
- `frontend/src/components/account/BulkEditAccountModal.vue`
- `frontend/src/components/account/AccountUsageCell.vue`
- `frontend/src/components/account/AccountTestModal.vue`
- `frontend/src/components/admin/account/ReAuthAccountModal.vue`

创建账号弹窗通常需要新增：

- 平台按钮
- 账号类型选项
- credentials 表单字段
- extra 表单字段
- 默认 `base_url`
- 默认 `model_mapping`
- 表单提交前的 `buildCredentials` 逻辑

### 4. 分组和渠道管理

重点修改：

- `frontend/src/views/admin/GroupsView.vue`
- `frontend/src/views/admin/ChannelsView.vue`
- `frontend/src/composables/useModelWhitelist.ts`

需要补：

- 分组平台选项
- 平台特定配置显隐
- 新平台模型白名单
- 新平台模型映射预设
- 渠道平台标签
- 定价输入的默认模型建议

### 5. Ops、订阅和用量页面

搜索平台硬编码：

```bash
rg "anthropic|openai|gemini|antigravity" frontend/src
```

重点检查：

- Ops dashboard 平台筛选
- usage dashboard 平台筛选
- subscription/redeem 分组平台筛选
- 首页或说明文案
- i18n 中的平台名称

## 八、数据库和迁移

多数平台字段是 `VARCHAR(50)`，新增平台通常不需要改字段类型。

但以下情况需要 migration：

| 场景 | 是否需要迁移 |
|------|--------------|
| 只是新增平台字符串 | 通常不需要 |
| 新增账号凭证字段 | 通常不需要，存在 JSON 中 |
| 新增 group/account/channel 独立列 | 需要 migration |
| 新增 usage 字段 | 需要 migration、repository、前端展示同步 |
| 平台重命名 | 需要 migration 更新历史数据 |
| 新增索引或统计维度 | 需要 migration |

迁移文件在：

- `backend/migrations`

注意迁移编号不要冲突，项目中已有大量 migration。

## 九、测试建议

### 后端单元测试

至少补：

- 平台常量/校验测试
- 请求解析测试
- 模型映射测试
- 上游请求构造测试
- 非流式响应 usage 提取测试
- 流式响应 usage 提取测试
- 401/403/429/5xx 错误测试
- failover 测试
- channel 平台定价隔离测试
- account/group 绑定平台一致性测试

参考现有文件：

- `backend/internal/service/gateway_request_test.go`
- `backend/internal/service/gateway_multiplatform_test.go`
- `backend/internal/service/openai_gateway_service_test.go`
- `backend/internal/service/gemini_messages_compat_service_test.go`
- `backend/internal/service/antigravity_gateway_service_test.go`
- `backend/internal/service/channel_service_test.go`

### 前端测试

至少补：

- 账号创建表单能选择新平台
- credentials 构造正确
- 编辑账号不会丢字段
- 平台筛选包含新平台
- 分组创建/编辑能选择新平台
- 渠道映射和定价支持新平台

参考现有文件：

- `frontend/src/components/account/__tests__`
- `frontend/src/views/admin/__tests__`
- `frontend/src/composables/__tests__`

### 本地验证命令

```bash
# 后端
cd backend
go test ./internal/service ./internal/handler ./internal/server

# 前端
cd frontend
pnpm run type-check
pnpm test
```

如果只改了文档，不需要跑完整测试；如果改了平台枚举或前端类型，至少跑前端 type-check。

## 十、接入完成前检查表

### 后端

- [ ] `domain` 和 `service` 平台常量已新增
- [ ] 分组创建/更新平台校验已放开
- [ ] 账号创建/编辑支持新平台 credentials
- [ ] 路由已能把请求分发到正确 handler/service
- [ ] endpoint 标准化和 upstream endpoint 推导正确
- [ ] 请求解析覆盖新平台 body
- [ ] 调度只选择新平台账号，除非有显式混合调度开关
- [ ] 模型映射支持精确和通配符
- [ ] 上游鉴权、代理、TLS 指纹、超时逻辑可用
- [ ] 非流式和流式响应都能返回客户端
- [ ] usage、计费、channel mapping、ops 字段完整
- [ ] 401/403/429/5xx 错误处理符合预期
- [ ] account test 可用

### 前端

- [ ] `GroupPlatform`、`AccountPlatform` 已更新
- [ ] 平台颜色、图标、徽标可显示
- [ ] 账号列表筛选包含新平台
- [ ] 创建/编辑账号表单支持新平台
- [ ] 分组管理支持新平台
- [ ] 渠道管理支持新平台模型映射和定价
- [ ] i18n 文案补齐
- [ ] Ops/Usage/Subscription/Redeem 相关筛选无遗漏

### 数据与文档

- [ ] 如需 migration，已添加并验证
- [ ] README 或部署说明中补充新平台配置示例
- [ ] 示例 credentials 中没有真实密钥
- [ ] 新平台默认模型、默认 base URL、限制说明已写清楚

## 十一、常见坑

### 坑 1：只加后端常量，前端类型没加

前端 `AccountPlatform` 和 `GroupPlatform` 是联合类型。后端可以存新平台，但前端会出现类型错误、筛选缺项、图标 fallback 或表单无法选择。

### 坑 2：分组平台支持了，调度没有隔离

调度必须保证 `group.platform` 和 `account.platform` 匹配。新平台默认不要跨平台调度。

### 坑 3：能转发但没有 usage

这会导致余额扣费、订阅限额、用量统计、dashboard 都不正确。新增转发链路必须返回足够的 usage 信息。

### 坑 4：流式响应失败后继续 failover

如果已经向客户端写了部分 SSE，再切换账号会造成响应流拼接。现有代码会记录 `streamStarted` 或 writer size，新平台也要遵守这个原则。

### 坑 5：模型映射链路不一致

项目同时存在账号级映射、渠道级映射、平台默认映射。新增平台时要明确优先级，并记录 requested/upstream/channel mapped model，避免计费模型和实际上游模型不一致。

### 坑 6：把新供应商都做成新平台

如果供应商只是 OpenAI-compatible 或 Anthropic-compatible，直接新增平台会带来大量重复 UI、调度、计费和测试成本。优先复用现有平台，加 `base_url` 和 `model_mapping`。

## 十二、推荐最小落地策略

对于大多数第三方模型供应商，推荐按这个顺序落地：

1. 先用现有 `anthropic`、`openai` 或 `gemini` 平台接入。
2. 通过账号 `credentials.base_url` 指向第三方上游。
3. 通过 `credentials.model_mapping` 做模型名转换。
4. 如需特殊鉴权，新增账号类型或 credentials 字段。
5. 只有当请求/响应协议无法复用时，再新增平台。

这样可以最大化复用项目已有能力，避免在调度、计费、前端和测试上重复造一套平台链路。
