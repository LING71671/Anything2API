# 新平台接入：人类最少工作指南与 AI 提示词

> 目标：人类只负责少量必须由人确认的事情，其余交给 AI 调研、设计、编码、测试和写文档。
>
> 使用原则：不要把真实密钥、Cookie、Refresh Token、生产账号信息发给 AI。所有样例都要脱敏。

## 一、人类只需要做什么

### 你真正需要提供的 5 类信息

| 你提供 | 最小要求 | 示例 |
|--------|----------|------|
| 平台名称和目标 | 说明要接入哪个平台、为什么接入 | `接入 FooAI，目标是让用户继续使用 /v1/messages` |
| 官方文档入口 | 给一个官方 API 文档 URL；没有 URL 就给文档截图/复制文本 | `https://docs.foo.ai/api` |
| 真实业务选择 | 确认是复用现有协议，还是必须新增平台 | `FooAI 兼容 OpenAI Chat，但我们希望走 Claude Code` |
| 脱敏请求/响应样例 | 至少一组成功响应；如支持流式，再给一组流式样例 | 把 `api_key` 替换成 `sk-REDACTED` |
| 私密配置自己保管 | API Key、OAuth Secret、组织 ID 等只放本地环境变量或后台 | 不要直接贴给 AI |

### 你不需要自己写的东西

AI 可以负责：

- 查官方 API 文档
- 判断是否能复用 `anthropic`、`openai`、`gemini`
- 设计接入方案
- 修改后端平台常量、路由、调度、转发服务
- 修改前端账号/分组/渠道管理页面
- 写模型映射、usage 解析、错误处理
- 写单元测试、mock upstream、文档和配置示例
- 整理接入 PR 说明

## 二、最快决策：不要急着新增平台

先问这三个问题：

1. 新平台是否兼容 OpenAI API？
2. 新平台是否兼容 Anthropic Messages API？
3. 新平台是否兼容 Gemini Native API？

如果任意一个答案是“是”，优先复用现有平台：

| 兼容协议 | 推荐接法 |
|----------|----------|
| OpenAI-compatible | 复用 `openai` 平台，配置 `base_url` 和 `api_key` |
| Anthropic-compatible | 复用 `anthropic` 平台，配置 `base_url`、`api_key`、`model_mapping` |
| Gemini-compatible | 复用 `gemini` 平台，配置 `base_url`、`api_key` |
| 特殊签名但入站仍是 Claude | 参考 Bedrock：做成 `anthropic` 下的新账号类型 |
| 协议完全不同 | 才新增独立平台 |

一句话：**能复用就复用，不能复用再新增平台。**

## 三、给 AI 的总提示词

把下面整段复制给 AI，然后按实际情况填空。

```md
你现在是 sub2api 项目的资深后端+前端工程师。请帮我接入一个新模型平台，但要先判断是否真的需要新增平台，优先复用现有 anthropic/openai/gemini 能力，只有协议无法复用时才新增平台。

## 项目背景
项目：sub2api
技术栈：Go + Gin 后端，Vue3 + TypeScript 前端
当前平台：anthropic、openai、gemini、antigravity
重要约束：
- 平台字段贯穿 accounts、groups、channels、usage、ops、前端管理页
- 新平台默认必须和其他平台调度隔离
- 不允许泄露真实密钥
- 所有新增代码必须有必要测试
- 如果只是 OpenAI/Anthropic/Gemini compatible，优先用 base_url + model_mapping 接入，不要新增平台

## 新平台信息
平台名称：
官方文档 URL：
是否兼容 OpenAI：
是否兼容 Anthropic：
是否兼容 Gemini：
我希望用户侧使用的入口：
- [ ] /v1/messages
- [ ] /v1/chat/completions
- [ ] /v1/responses
- [ ] /v1beta/models
- [ ] 新增专属入口：

## 鉴权
鉴权方式：
Header 格式：
是否需要 OAuth：
是否需要刷新 token：
需要哪些 credentials 字段：
需要哪些 extra 字段：

## API
Base URL：
文本生成 endpoint：
模型列表 endpoint：
是否支持 stream：
stream 格式是 SSE / NDJSON / WebSocket / 其他：
是否有 usage/余额接口：

## 模型
希望暴露给用户的模型名：
实际上游模型名：
默认模型映射：

## 样例
成功请求 JSON：
成功响应 JSON：
流式响应样例：
错误响应样例：

## 业务策略
401 如何处理：
403 如何处理：
429 如何处理：
5xx 是否 failover：
是否需要账号测试：
是否需要前端创建账号表单：
是否需要渠道定价：

## 你的任务
1. 先阅读项目现有代码，定位平台常量、账号/分组、路由、gateway service、usage、前端平台枚举。
2. 先给出接入路线判断：复用现有平台还是新增平台，并说明原因。
3. 列出最小改动范围。
4. 如果信息不足，最多问我 5 个必须由人类回答的问题；不要问能从官方文档或代码里查到的问题。
5. 在信息足够后直接实现代码。
6. 补必要测试。
7. 最后给我一份变更总结、测试结果、还需要我手动配置的私密信息清单。
```

## 四、分阶段提示词

如果你不想一次性让 AI 改代码，可以按下面阶段走。

### 阶段 1：让 AI 做可行性判断

```md
请只做调研和方案判断，不要改代码。

目标：判断新平台是否应该作为独立平台接入，还是复用 sub2api 现有 anthropic/openai/gemini 平台。

请完成：
1. 阅读项目平台接入相关代码。
2. 查官方文档，只使用官方文档或官方 SDK/GitHub 作为依据。
3. 判断该平台是否兼容 OpenAI、Anthropic 或 Gemini。
4. 给出推荐接入路线：
   - 复用现有平台
   - 新增账号类型
   - 新增独立平台
5. 列出需要人类补充的信息，不超过 5 条。
6. 列出后续代码改动清单，但暂时不要实现。

平台名称：
官方文档 URL：
目标入口：
```

### 阶段 2：让 AI 写设计方案

```md
请基于上一阶段结论，写一份 sub2api 新平台接入设计方案，暂时不要改代码。

方案必须包含：
1. 是否新增平台常量，原因是什么。
2. 是否新增账号类型，原因是什么。
3. 后端改动文件清单。
4. 前端改动文件清单。
5. credentials/extra 字段设计。
6. 模型映射策略。
7. usage/计费字段映射。
8. 错误处理策略，尤其是 401/403/429/5xx。
9. 流式响应处理方案。
10. 测试计划。
11. 人类需要手动配置的内容。

要求：
- 优先最小改动。
- 不要引入不必要抽象。
- 保持平台调度隔离。
- 明确哪些内容来自官方文档，哪些是根据项目代码推断。
```

### 阶段 3：让 AI 实现最小闭环

```md
请按设计方案实现最小可用闭环。

本阶段目标：
- 能创建/配置该平台账号
- 能通过目标入口发送一次非流式请求
- 能正确选择该平台账号
- 能返回响应
- 能记录 usage 或在无法解析 usage 时有明确 fallback
- 有必要单元测试

实现要求：
1. 修改范围尽量小。
2. 不要重构无关代码。
3. 不要破坏现有 anthropic/openai/gemini/antigravity 行为。
4. 新平台默认不参与混合调度。
5. 所有真实密钥使用占位符或环境变量。
6. 完成后运行相关测试，并报告结果。
```

### 阶段 4：让 AI 补流式、错误和计费

```md
请在最小闭环基础上补齐生产可用能力。

需要实现：
1. 流式响应处理。
2. 401/403/429/5xx 错误处理。
3. failover 策略，注意流式响应一旦写出内容就不能切换账号。
4. usage 提取和计费字段映射。
5. channel 模型映射和定价支持。
6. account test 支持。
7. 对应测试。

请先列出风险点，再实现。
```

### 阶段 5：让 AI 补前端和文档

```md
请补齐新平台的前端管理体验和文档。

需要实现：
1. AccountPlatform / GroupPlatform 类型。
2. 平台颜色、图标、PlatformTypeBadge。
3. 账号列表筛选。
4. 创建/编辑账号表单。
5. 分组管理平台选项。
6. 渠道管理模型映射/定价。
7. i18n 文案。
8. 配置示例和排障说明。

要求：
- UI 只展示用户真正需要配置的字段。
- 不暴露敏感字段明文。
- 如果该平台只是复用现有平台，不要新增多余 UI。
```

## 五、让 AI 上网查资料的提示词

当平台文档公开时，用这个提示词：

```md
请上网查这个平台的官方 API 文档，并只引用官方来源或官方 SDK/GitHub。

平台：
文档 URL：

请整理：
1. Base URL
2. 文本生成 endpoint
3. 请求 JSON 字段
4. 非流式响应 JSON 字段
5. 流式协议格式
6. usage 字段
7. 鉴权方式
8. 模型列表和模型能力
9. 错误响应格式
10. 是否兼容 OpenAI/Anthropic/Gemini

输出要求：
- 给出官方链接。
- 明确哪些信息没有在官方文档中找到。
- 不要根据第三方文章做最终判断。
- 最后给出 sub2api 推荐接入方式。
```

## 六、让 AI 读代码的提示词

```md
请阅读当前 sub2api 代码，找出新增平台需要改哪些地方。

重点搜索：
- PlatformAnthropic / PlatformOpenAI / PlatformGemini / PlatformAntigravity
- AccountPlatform / GroupPlatform
- RegisterGatewayRoutes
- ParseGatewayRequest
- GatewayService.Forward
- OpenAIGatewayService.Forward
- GeminiMessagesCompatService.Forward
- AntigravityGatewayService.Forward
- channel platform pricing
- frontend platform options

输出：
1. 当前平台架构说明。
2. 新增平台最小文件清单。
3. 如果复用现有平台，最小配置方式。
4. 如果新增独立平台，完整改动链路。
5. 不要改代码。
```

## 七、人类收集样例的最小模板

你可以把下面填好给 AI。密钥全部脱敏。

```md
## 平台
名称：
官方文档：

## 目标
我希望用户继续使用：
我希望后台管理方式：

## 鉴权
鉴权 Header 示例：
需要的非敏感字段：
敏感字段我会本地配置，不贴给 AI：

## 请求样例
curl：
请求 JSON：

## 响应样例
非流式成功响应：
流式成功响应：
错误响应 401：
错误响应 403：
错误响应 429：
错误响应 5xx：

## 模型
用户请求模型：
上游实际模型：
模型映射：

## 计费
usage 字段：
平台价格或我的内部价格：
```

## 八、给 AI 的验收提示词

```md
请做一次新平台接入代码审查，重点找遗漏，不要只总结。

检查项：
1. 平台常量是否前后端一致。
2. group/account/channel/usage 是否都支持新平台。
3. 调度是否默认平台隔离。
4. 是否误伤现有四个平台。
5. 是否有真实密钥或敏感信息进入代码、测试、文档。
6. 非流式和流式是否都处理。
7. 401/403/429/5xx 是否有策略。
8. usage 和计费是否完整。
9. 前端创建/编辑/筛选/i18n 是否完整。
10. 测试是否覆盖关键路径。

输出格式：
- Findings：按严重程度列问题，带文件和行号。
- Required fixes：必须修复项。
- Nice to have：可选优化。
- Test gaps：测试缺口。
```

## 九、真实密钥处理规则

不要给 AI：

- API Key
- Refresh Token
- Access Token
- Cookie
- OAuth Client Secret
- 私有证书
- 生产组织 ID 和敏感项目 ID
- 用户真实请求内容

可以给 AI：

- 脱敏后的 Header 结构
- 脱敏后的 JSON 字段名
- 本地环境变量名称
- 官方文档 URL
- mock 响应
- 测试账号的非敏感说明

推荐写法：

```json
{
  "api_key": "sk-REDACTED",
  "organization_id": "org-REDACTED",
  "project_id": "project-demo"
}
```

## 十、最省事的实际流程

1. 你把平台名和官方文档 URL 给 AI。
2. AI 判断是否复用现有平台。
3. 你确认业务选择：复用还是新增。
4. 你给一组脱敏请求/响应样例。
5. AI 写代码和测试。
6. 你在本地填真实密钥跑一次真实请求。
7. AI 根据真实报错继续修。

人类最少要做的事情，其实就是：

- 给官方文档入口
- 给脱敏样例
- 保管真实密钥
- 做最终真实账号验证

其余都可以交给 AI。
