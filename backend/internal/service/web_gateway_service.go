package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/util/urlvalidator"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

const defaultWebRequestTemplate = `{"model":"{{model}}","prompt":"{{prompt}}"}`

// WebGatewayService adapts generic HTTP/JSON upstreams to the gateway response shapes.
type WebGatewayService struct {
	httpUpstream     HTTPUpstream
	cfg              *config.Config
	rateLimitService *RateLimitService
	platformService  *PlatformService
	oauthService     *WebSourceOAuthService
	onyxSessions     *webOnyxSessionStore
	onyxSharedStore  OnyxSessionStore
}

func NewWebGatewayService(httpUpstream HTTPUpstream, cfg *config.Config, rateLimitService *RateLimitService, platformService *PlatformService, onyxSharedStore OnyxSessionStore, oauthServices ...*WebSourceOAuthService) *WebGatewayService {
	var oauthService *WebSourceOAuthService
	if len(oauthServices) > 0 {
		oauthService = oauthServices[0]
	}
	return &WebGatewayService{
		httpUpstream:     httpUpstream,
		cfg:              cfg,
		rateLimitService: rateLimitService,
		platformService:  platformService,
		oauthService:     oauthService,
		onyxSessions:     newWebOnyxSessionStore(stickySessionTTL),
		onyxSharedStore:  onyxSharedStore,
	}
}

type webForwardFormat string

const (
	webForwardAnthropic       webForwardFormat = "anthropic"
	webForwardChatCompletions webForwardFormat = "chat_completions"
	webForwardResponses       webForwardFormat = "responses"
)

type webAuthConfig struct {
	Type   string `json:"type"`
	Header string `json:"header"`
	Token  string `json:"token"`
}

type webUsagePaths struct {
	InputTokens              string `json:"input_tokens"`
	OutputTokens             string `json:"output_tokens"`
	CacheCreationInputTokens string `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     string `json:"cache_read_input_tokens"`
}

type webToolCallConfig struct {
	Path            string `json:"path"`
	IDPath          string `json:"id_path"`
	NamePath        string `json:"name_path"`
	ArgumentsPath   string `json:"arguments_path"`
	ArgumentsFormat string `json:"arguments_format"`
}

type webStreamConfig struct {
	Format     string `json:"format"`
	DoneMarker string `json:"done_marker"`
	TextPath   string `json:"text_path"`
}

type webResponseConfig struct {
	TextPath       string            `json:"text_path"`
	RequestIDPath  string            `json:"request_id_path"`
	ModelPath      string            `json:"model_path"`
	StopReasonPath string            `json:"stop_reason_path"`
	UsagePaths     webUsagePaths     `json:"usage_paths"`
	ToolCalls      webToolCallConfig `json:"tool_calls"`
	Stream         webStreamConfig   `json:"stream"`
}

type webCredentials struct {
	BaseURL               string            `json:"base_url"`
	Method                string            `json:"method"`
	Headers               map[string]string `json:"headers"`
	Auth                  webAuthConfig     `json:"auth"`
	RequestTemplate       string            `json:"request_template"`
	StreamRequestTemplate string            `json:"stream_request_template"`
	SupportsStream        bool              `json:"supports_stream"`
	UpstreamFormat        string            `json:"upstream_format"`
	Response              webResponseConfig `json:"response"`
	ModelMapping          map[string]any    `json:"model_mapping"`
}

type webPromptInput struct {
	Body               []byte
	Model              string
	Stream             bool
	Messages           []any
	System             any
	MetadataUserID     string
	SessionHash        string
	GroupID            *int64
	EndpointFormat     webForwardFormat
	OnUpstreamAccepted func()
}

type webForwardOutput struct {
	RequestID      string
	Text           string
	Model          string
	UpstreamModel  string
	Usage          ClaudeUsage
	UsageEstimated bool
	Duration       time.Duration
	Stream         bool
}

func (s *WebGatewayService) Forward(ctx context.Context, c *gin.Context, account *Account, parsed *ParsedRequest) (*ForwardResult, error) {
	if parsed == nil {
		return nil, fmt.Errorf("web forward: empty parsed request")
	}
	out, err := s.forward(ctx, c, account, webPromptInput{
		Body:               parsed.Body,
		Model:              parsed.Model,
		Stream:             parsed.Stream,
		Messages:           parsed.Messages,
		System:             parsed.System,
		MetadataUserID:     parsed.MetadataUserID,
		SessionHash:        parsed.SessionHash,
		GroupID:            parsed.GroupID,
		EndpointFormat:     webForwardAnthropic,
		OnUpstreamAccepted: parsed.OnUpstreamAccepted,
	}, webForwardAnthropic)
	if err != nil {
		return nil, err
	}
	return &ForwardResult{
		RequestID:      out.RequestID,
		Usage:          out.Usage,
		Model:          parsed.Model,
		UpstreamModel:  out.UpstreamModel,
		Stream:         out.Stream,
		Duration:       out.Duration,
		UsageEstimated: out.UsageEstimated,
	}, nil
}

func (s *WebGatewayService) ForwardAsChatCompletions(ctx context.Context, c *gin.Context, account *Account, body []byte, parsed *ParsedRequest) (*ForwardResult, error) {
	model, stream := webModelAndStream(body, parsed)
	out, err := s.forward(ctx, c, account, webPromptInput{
		Body:           body,
		Model:          model,
		Stream:         stream,
		Messages:       webExtractMessages(body),
		System:         gjson.GetBytes(body, "system").Value(),
		MetadataUserID: gjson.GetBytes(body, "metadata.user_id").String(),
		SessionHash:    parsedSessionHash(parsed),
		GroupID:        parsedGroupID(parsed),
		EndpointFormat: webForwardChatCompletions,
	}, webForwardChatCompletions)
	if err != nil {
		return nil, err
	}
	return &ForwardResult{
		RequestID:      out.RequestID,
		Usage:          out.Usage,
		Model:          model,
		UpstreamModel:  out.UpstreamModel,
		Stream:         out.Stream,
		Duration:       out.Duration,
		UsageEstimated: out.UsageEstimated,
	}, nil
}

func (s *WebGatewayService) ForwardAsResponses(ctx context.Context, c *gin.Context, account *Account, body []byte, parsed *ParsedRequest) (*ForwardResult, error) {
	model, stream := webModelAndStream(body, parsed)
	out, err := s.forward(ctx, c, account, webPromptInput{
		Body:           body,
		Model:          model,
		Stream:         stream,
		Messages:       webExtractResponsesInput(body),
		System:         gjson.GetBytes(body, "instructions").Value(),
		MetadataUserID: gjson.GetBytes(body, "metadata.user_id").String(),
		SessionHash:    parsedSessionHash(parsed),
		GroupID:        parsedGroupID(parsed),
		EndpointFormat: webForwardResponses,
	}, webForwardResponses)
	if err != nil {
		return nil, err
	}
	return &ForwardResult{
		RequestID:      out.RequestID,
		Usage:          out.Usage,
		Model:          model,
		UpstreamModel:  out.UpstreamModel,
		Stream:         out.Stream,
		Duration:       out.Duration,
		UsageEstimated: out.UsageEstimated,
	}, nil
}

func (s *WebGatewayService) forward(ctx context.Context, c *gin.Context, account *Account, input webPromptInput, format webForwardFormat) (*webForwardOutput, error) {
	start := time.Now()
	if account == nil || !s.isWebPlatform(ctx, account.Platform) {
		return nil, fmt.Errorf("web forward: invalid account platform")
	}
	if err := s.ensureFreshWebToken(ctx, account); err != nil {
		writeWebError(c, format, http.StatusBadGateway, "upstream_error", err.Error())
		return nil, err
	}

	var lastRespHeaders http.Header
	onyxRecoveryTriggered := false
	for attempt := 0; attempt < 2; attempt++ {
		creds, targetURL, upstreamModel, upstreamBody, onyxPrepared, err := s.prepareWebRequest(ctx, account, input)
		if err != nil {
			writeWebError(c, format, http.StatusBadRequest, "invalid_request_error", err.Error())
			return nil, err
		}
		if input.Stream && !creds.SupportsStream {
			writeWebError(c, format, http.StatusBadRequest, "invalid_request_error", "web2api HTTP adapter does not support stream=true for this platform")
			return nil, fmt.Errorf("web2api stream is not supported for platform %s", account.Platform)
		}

		req, err := http.NewRequestWithContext(ctx, creds.Method, targetURL, bytes.NewReader(upstreamBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("content-type", "application/json")
		if input.Stream {
			req.Header.Set("accept", "text/event-stream")
		}
		for k, v := range creds.Headers {
			if strings.TrimSpace(k) != "" {
				req.Header.Set(k, v)
			}
		}
		applyWebAuth(req, creds.Auth)

		proxyURL := ""
		if account.ProxyID != nil && account.Proxy != nil {
			proxyURL = account.Proxy.URL()
		}

		resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
		if err != nil {
			setOpsUpstreamError(c, 0, sanitizeUpstreamErrorMessage(err.Error()), "")
			return nil, fmt.Errorf("web upstream request failed: %w", err)
		}
		lastRespHeaders = resp.Header

		if resp.StatusCode >= 400 {
			respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
			_ = resp.Body.Close()
			if readErr != nil {
				return nil, fmt.Errorf("read web upstream response: %w", readErr)
			}
			if attempt == 0 && onyxPrepared != nil && onyxPrepared.Recoverable && s.shouldRetryOnyxInvalidSession(resp.StatusCode, respBody) {
				recordOnyxInvalidSessionRetry()
				onyxRecoveryTriggered = true
				s.logOnyxSessionDebug("onyx.invalid_session_recovery_triggered", "recovered", onyxPrepared, OnyxSessionBinding{
					ChatSessionID:   onyxPrepared.ChatSessionID,
					ParentMessageID: 0,
				}, zap.Int("status_code", resp.StatusCode))
				s.deleteOnyxSessionBinding(ctx, onyxPrepared)
				continue
			}
			if attempt == 0 && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) && s.refreshWebAccountAfterAuthError(ctx, account) == nil {
				continue
			}
			return nil, s.handleWebUpstreamError(ctx, c, account, resp.StatusCode, resp.Header, respBody, targetURL)
		}

		if input.OnUpstreamAccepted != nil {
			input.OnUpstreamAccepted()
			input.OnUpstreamAccepted = nil
		}

		if input.Stream {
			out, err := s.handleWebStreamingResponse(resp, c, creds, format, input, upstreamModel, start, onyxPrepared)
			_ = resp.Body.Close()
			if err == nil && onyxRecoveryTriggered && onyxPrepared != nil {
				recordOnyxInvalidSessionRetrySuccess()
				s.logOnyxSessionDebug("onyx.invalid_session_recovery_succeeded", "recovered", onyxPrepared, OnyxSessionBinding{
					ChatSessionID: onyxPrepared.ChatSessionID,
				})
			}
			return out, err
		}

		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read web upstream response: %w", readErr)
		}
		anthResp, upstreamRequestID, responseModel, usage, err := parseWebResponseToAnthropic(respBody, creds, input.Model, upstreamModel)
		if err != nil {
			setOpsUpstreamError(c, http.StatusBadGateway, err.Error(), "")
			return nil, &UpstreamFailoverError{
				StatusCode:      http.StatusBadGateway,
				ResponseBody:    []byte(err.Error()),
				ResponseHeaders: resp.Header,
			}
		}
		requestID := upstreamRequestID
		if requestID == "" {
			requestID = resp.Header.Get("x-request-id")
		}
		if requestID == "" {
			requestID = anthResp.ID
		}
		if requestID == "" {
			requestID = "web_" + uuid.NewString()
		}
		anthResp.ID = requestID
		if responseModel != "" {
			upstreamModel = responseModel
		}
		if onyxPrepared != nil {
			s.handleOnyxNonStreamingSessionState(ctx, account, creds, targetURL, onyxPrepared, respBody)
		}
		text := webTextFromAnthropicContent(anthResp.Content)
		usage, usageEstimated := estimateWebUsageIfMissing(usage, input, text)
		out := &webForwardOutput{
			RequestID:      requestID,
			Text:           text,
			Model:          input.Model,
			UpstreamModel:  optionalUpstreamModel(input.Model, upstreamModel),
			Usage:          usage,
			UsageEstimated: usageEstimated,
			Duration:       time.Since(start),
			Stream:         false,
		}
		if onyxRecoveryTriggered && onyxPrepared != nil {
			recordOnyxInvalidSessionRetrySuccess()
			s.logOnyxSessionDebug("onyx.invalid_session_recovery_succeeded", "recovered", onyxPrepared, OnyxSessionBinding{
				ChatSessionID: onyxPrepared.ChatSessionID,
			})
		}
		writeWebAnthropicSuccess(c, format, anthResp)
		return out, nil
	}

	return nil, &UpstreamFailoverError{
		StatusCode:      http.StatusBadGateway,
		ResponseBody:    []byte("web upstream authentication retry failed"),
		ResponseHeaders: lastRespHeaders,
	}
}

func parseWebCredentials(account *Account) (webCredentials, error) {
	var creds webCredentials
	if account == nil || account.Credentials == nil {
		return creds, fmt.Errorf("web credentials are required")
	}
	raw, err := json.Marshal(account.Credentials)
	if err != nil {
		return creds, fmt.Errorf("marshal web credentials: %w", err)
	}
	if err := json.Unmarshal(raw, &creds); err != nil {
		return creds, fmt.Errorf("parse web credentials: %w", err)
	}
	return normalizeWebCredentials(creds)
}

func (s *WebGatewayService) isWebPlatform(ctx context.Context, platform string) bool {
	if platform == PlatformWeb {
		return true
	}
	if s.platformService != nil {
		return s.platformService.IsWebSourcePlatform(ctx, platform)
	}
	return IsWebPlatformKey(platform)
}

func (s *WebGatewayService) resolveWebCredentials(ctx context.Context, account *Account) (webCredentials, error) {
	if account.Platform == PlatformWeb {
		return parseWebCredentials(account)
	}
	if s.platformService == nil {
		return webCredentials{}, fmt.Errorf("dynamic web platform registry is not configured")
	}
	platform, err := s.platformService.GetWebSourcePlatform(ctx, account.Platform)
	if err != nil {
		return webCredentials{}, fmt.Errorf("load dynamic web platform %q: %w", account.Platform, err)
	}
	return buildDynamicWebCredentials(platform, account)
}

func buildDynamicWebCredentials(platform *WebSourcePlatform, account *Account) (webCredentials, error) {
	var creds webCredentials
	if platform == nil {
		return creds, fmt.Errorf("dynamic web platform config is required")
	}
	reqCfg := platform.RequestConfig
	respCfg := platform.ResponseConfig
	creds.BaseURL = strings.TrimSpace(stringMapValue(reqCfg, "base_url"))
	creds.Method = strings.ToUpper(strings.TrimSpace(stringMapValue(reqCfg, "method")))
	creds.RequestTemplate = strings.TrimSpace(stringMapValue(reqCfg, "request_template"))
	creds.StreamRequestTemplate = strings.TrimSpace(stringMapValue(reqCfg, "stream_request_template"))
	creds.SupportsStream = boolMapValue(reqCfg, "supports_stream")
	creds.UpstreamFormat = normalizeWebUpstreamFormat(stringMapValue(reqCfg, "upstream_format"))
	creds.Headers = mergeStringMaps(anyStringMap(reqCfg["headers"]), anyStringMap(account.Credentials["headers"]))
	creds.ModelMapping = mergeWebModelMapping(
		defaultWebSourceModelMapping(platform.PlatformKey),
		platform.DefaultModelMapping,
		anyMap(account.Credentials["model_mapping"]),
	)
	creds.Response = webResponseConfig{
		TextPath:       strings.TrimSpace(stringMapValue(respCfg, "text_path")),
		RequestIDPath:  strings.TrimSpace(stringMapValue(respCfg, "request_id_path")),
		ModelPath:      strings.TrimSpace(stringMapValue(respCfg, "model_path")),
		StopReasonPath: strings.TrimSpace(stringMapValue(respCfg, "stop_reason_path")),
		UsagePaths: webUsagePaths{
			InputTokens:              strings.TrimSpace(stringMapValue(anyMap(respCfg["usage_paths"]), "input_tokens")),
			OutputTokens:             strings.TrimSpace(stringMapValue(anyMap(respCfg["usage_paths"]), "output_tokens")),
			CacheCreationInputTokens: strings.TrimSpace(stringMapValue(anyMap(respCfg["usage_paths"]), "cache_creation_input_tokens")),
			CacheReadInputTokens:     strings.TrimSpace(stringMapValue(anyMap(respCfg["usage_paths"]), "cache_read_input_tokens")),
		},
		ToolCalls: webToolCallConfig{
			Path:            strings.TrimSpace(stringMapValue(anyMap(respCfg["tool_calls"]), "path")),
			IDPath:          strings.TrimSpace(stringMapValue(anyMap(respCfg["tool_calls"]), "id_path")),
			NamePath:        strings.TrimSpace(stringMapValue(anyMap(respCfg["tool_calls"]), "name_path")),
			ArgumentsPath:   strings.TrimSpace(stringMapValue(anyMap(respCfg["tool_calls"]), "arguments_path")),
			ArgumentsFormat: strings.TrimSpace(stringMapValue(anyMap(respCfg["tool_calls"]), "arguments_format")),
		},
		Stream: webStreamConfig{
			Format:     strings.TrimSpace(stringMapValue(anyMap(respCfg["stream"]), "format")),
			DoneMarker: strings.TrimSpace(stringMapValue(anyMap(respCfg["stream"]), "done_marker")),
			TextPath:   strings.TrimSpace(stringMapValue(anyMap(respCfg["stream"]), "text_path")),
		},
	}
	token := firstString(account.Credentials, "access_token", "token", "session_token", "api_key")
	switch platform.AuthMode {
	case WebAuthModeBearer, WebAuthModeOAuth2PKCE:
		creds.Auth = webAuthConfig{Type: "bearer", Token: token}
	case WebAuthModeHeader:
		header := firstString(reqCfg, "auth_header", "header")
		if header == "" {
			header = firstString(account.Credentials, "auth_header", "header")
		}
		creds.Auth = webAuthConfig{Type: "header", Header: header, Token: token}
	case WebAuthModeCookie:
		if cookie := firstString(account.Credentials, "cookie", "session_token"); cookie != "" {
			if creds.Headers == nil {
				creds.Headers = map[string]string{}
			}
			creds.Headers["cookie"] = cookie
		}
		creds.Auth = webAuthConfig{Type: "none"}
	default:
		creds.Auth = webAuthConfig{Type: "none"}
	}
	return normalizeWebCredentials(creds)
}

func mergeWebModelMapping(maps ...map[string]any) map[string]any {
	out := map[string]any{}
	for _, m := range maps {
		for k, v := range m {
			if strings.TrimSpace(k) != "" {
				out[k] = v
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeWebCredentials(creds webCredentials) (webCredentials, error) {
	creds.BaseURL = strings.TrimSpace(creds.BaseURL)
	if creds.BaseURL == "" {
		return creds, fmt.Errorf("web credentials base_url is required")
	}
	creds.Method = strings.ToUpper(strings.TrimSpace(creds.Method))
	if creds.Method == "" {
		creds.Method = http.MethodPost
	}
	switch creds.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
	default:
		return creds, fmt.Errorf("web credentials method must be POST, PUT, or PATCH")
	}
	if strings.TrimSpace(creds.RequestTemplate) == "" {
		creds.RequestTemplate = defaultWebRequestTemplate
	}
	creds.UpstreamFormat = normalizeWebUpstreamFormat(creds.UpstreamFormat)
	if strings.TrimSpace(creds.StreamRequestTemplate) == "" {
		creds.StreamRequestTemplate = creds.RequestTemplate
	}
	creds.Response.ToolCalls.ArgumentsFormat = strings.ToLower(strings.TrimSpace(creds.Response.ToolCalls.ArgumentsFormat))
	if creds.Response.ToolCalls.ArgumentsFormat == "" {
		creds.Response.ToolCalls.ArgumentsFormat = "json_string"
	}
	creds.Response.Stream.Format = normalizeWebStreamFormat(creds.Response.Stream.Format, creds.UpstreamFormat)
	if strings.TrimSpace(creds.Response.TextPath) == "" && strings.TrimSpace(creds.Response.ToolCalls.Path) == "" {
		if creds.UpstreamFormat == "custom_json" {
			return creds, fmt.Errorf("web credentials response.text_path or response.tool_calls.path is required")
		}
	}
	creds.Auth.Type = strings.ToLower(strings.TrimSpace(creds.Auth.Type))
	if creds.Auth.Type == "" {
		creds.Auth.Type = "none"
	}
	return creds, nil
}

func (s *WebGatewayService) validateWebURL(raw string) (string, error) {
	if s.cfg != nil && s.cfg.Security.URLAllowlist.Enabled {
		return urlvalidator.ValidateHTTPURL(raw, s.cfg.Security.URLAllowlist.AllowInsecureHTTP, urlvalidator.ValidationOptions{
			AllowedHosts:     s.cfg.Security.URLAllowlist.UpstreamHosts,
			RequireAllowlist: true,
			AllowPrivate:     s.cfg.Security.URLAllowlist.AllowPrivateHosts,
		})
	}
	allowHTTP := false
	if s.cfg != nil {
		allowHTTP = s.cfg.Security.URLAllowlist.AllowInsecureHTTP
	}
	return urlvalidator.ValidateURLFormat(raw, allowHTTP)
}

func (s *WebGatewayService) prepareWebRequest(ctx context.Context, account *Account, input webPromptInput) (webCredentials, string, string, []byte, *webOnyxPreparedRequest, error) {
	creds, err := s.resolveWebCredentials(ctx, account)
	if err != nil {
		return creds, "", "", nil, nil, err
	}
	targetURL, err := s.validateWebURL(creds.BaseURL)
	if err != nil {
		return creds, "", "", nil, nil, err
	}

	upstreamModel := mapWebRequestedModel(input.Model, account, creds)
	template := creds.RequestTemplate
	if input.Stream && strings.TrimSpace(creds.StreamRequestTemplate) != "" {
		template = creds.StreamRequestTemplate
	}
	values := buildWebTemplateValues(input, upstreamModel)
	upstreamBody, err := buildWebRequestBody(template, values)
	if err != nil {
		return creds, "", "", nil, nil, err
	}
	onyxPrepared, upstreamBody, err := s.prepareOnyxSessionAwareRequest(ctx, account, creds, targetURL, upstreamBody, input)
	if err != nil {
		return creds, "", "", nil, nil, err
	}
	return creds, targetURL, upstreamModel, upstreamBody, onyxPrepared, nil
}

func parsedSessionHash(parsed *ParsedRequest) string {
	if parsed == nil {
		return ""
	}
	return strings.TrimSpace(parsed.SessionHash)
}

func parsedGroupID(parsed *ParsedRequest) *int64 {
	if parsed == nil {
		return nil
	}
	return parsed.GroupID
}

func (s *WebGatewayService) prepareOnyxSessionAwareRequest(ctx context.Context, account *Account, creds webCredentials, targetURL string, upstreamBody []byte, input webPromptInput) (*webOnyxPreparedRequest, []byte, error) {
	if !s.isOnyxSessionAwareRequest(creds, targetURL) {
		return nil, upstreamBody, nil
	}
	sessionKey := s.onyxSessionKey(account, input)
	if sessionKey == "" {
		recordOnyxSessionStatelessBypass()
		return nil, upstreamBody, nil
	}
	groupID := derefGroupID(input.GroupID)
	sessionHash := strings.TrimSpace(input.SessionHash)

	prepared := &webOnyxPreparedRequest{
		SessionKey:            sessionKey,
		GroupID:               groupID,
		AccountID:             account.ID,
		SessionHash:           sessionHash,
		ExplicitChatSessionID: strings.TrimSpace(gjson.GetBytes(upstreamBody, "chat_session_id").String()) != "",
		ExplicitParentMessage: gjson.GetBytes(upstreamBody, "parent_message_id").Exists(),
	}
	binding, bindingSource, hasBinding := s.getOnyxSessionBinding(ctx, prepared)
	prepared.BindingSource = bindingSource
	explicitChatSessionID := strings.TrimSpace(gjson.GetBytes(upstreamBody, "chat_session_id").String())
	chatSessionID := explicitChatSessionID

	if chatSessionID == "" {
		if hasBinding && binding.ChatSessionID != "" {
			chatSessionID = binding.ChatSessionID
		}
		if chatSessionID == "" {
			createdChatSessionID, err := s.createOnyxChatSession(ctx, account, creds, targetURL, upstreamBody, input)
			if err != nil {
				return nil, nil, err
			}
			chatSessionID = createdChatSessionID
			prepared.BindingSource = "new"
			prepared.Recoverable = true
		}
		if chatSessionID != "" {
			updatedBody, err := sjson.SetBytes(upstreamBody, "chat_session_id", chatSessionID)
			if err != nil {
				return nil, nil, fmt.Errorf("inject onyx chat_session_id: %w", err)
			}
			upstreamBody = updatedBody
		}
	}

	parentAllowed := !prepared.ExplicitParentMessage
	if explicitChatSessionID != "" && hasBinding && binding.ChatSessionID != "" && binding.ChatSessionID != explicitChatSessionID {
		parentAllowed = false
	}
	if parentAllowed && hasBinding {
		parentMessageID := int64(0)
		if binding.ReservedAssistantMessageID > 0 {
			parentMessageID = binding.ReservedAssistantMessageID
		} else if binding.ParentMessageID > 0 {
			parentMessageID = binding.ParentMessageID
		}
		if parentMessageID > 0 {
			updatedBody, err := sjson.SetBytes(upstreamBody, "parent_message_id", parentMessageID)
			if err != nil {
				return nil, nil, fmt.Errorf("inject onyx parent_message_id: %w", err)
			}
			upstreamBody = updatedBody
			if explicitChatSessionID == "" {
				prepared.Recoverable = true
			}
		}
	}

	prepared.ChatSessionID = chatSessionID
	return prepared, upstreamBody, nil
}

func (s *WebGatewayService) isOnyxSessionAwareRequest(creds webCredentials, targetURL string) bool {
	if normalizeWebStreamFormat(creds.Response.Stream.Format, creds.UpstreamFormat) != "onyx_chat_sse" {
		return false
	}
	u, err := url.Parse(targetURL)
	if err != nil {
		return false
	}
	return strings.HasSuffix(strings.TrimRight(u.Path, "/"), "/chat/send-chat-message")
}

func (s *WebGatewayService) onyxSessionKey(account *Account, input webPromptInput) string {
	if account == nil {
		return ""
	}
	sessionHash := strings.TrimSpace(input.SessionHash)
	if sessionHash == "" {
		return ""
	}
	return fmt.Sprintf("%d:%d:%s", derefGroupID(input.GroupID), account.ID, sessionHash)
}

func (s *WebGatewayService) getOnyxSessionBinding(ctx context.Context, prepared *webOnyxPreparedRequest) (OnyxSessionBinding, string, bool) {
	if s == nil || prepared == nil || strings.TrimSpace(prepared.SessionKey) == "" {
		return OnyxSessionBinding{}, "", false
	}
	if binding, ok := s.onyxSessions.Get(prepared.SessionKey); ok {
		recordOnyxSessionLocalHotHit()
		return binding, "local", true
	}
	if s.onyxSharedStore == nil || prepared.AccountID <= 0 || strings.TrimSpace(prepared.SessionHash) == "" {
		return OnyxSessionBinding{}, "", false
	}
	cacheCtx, cancel := withOnyxSessionStoreRedisTimeout(ctx)
	defer cancel()
	binding, err := s.onyxSharedStore.Get(cacheCtx, prepared.GroupID, prepared.AccountID, prepared.SessionHash)
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			recordOnyxSessionSharedStoreGetError()
			recordOnyxSharedStoreDegradedRead()
			s.logOnyxSessionError("onyx.shared_session_get_failed", "shared", prepared, OnyxSessionBinding{}, err)
		} else {
			recordOnyxSessionSharedStoreMiss()
		}
		return OnyxSessionBinding{}, "", false
	}
	binding = normalizeOnyxSessionBinding(binding)
	if onyxSessionBindingEmpty(binding) {
		recordOnyxSessionSharedStoreMiss()
		return OnyxSessionBinding{}, "", false
	}
	recordOnyxSessionSharedStoreHit()
	s.onyxSessions.Upsert(prepared.SessionKey, binding)
	s.logOnyxSessionDebug("onyx.shared_session_rehydrated", "shared", prepared, binding)
	return binding, "shared", true
}

func (s *WebGatewayService) upsertOnyxSessionBinding(ctx context.Context, sessionKey string, groupID, accountID int64, sessionHash string, update OnyxSessionBinding) OnyxSessionBinding {
	if s == nil || s.onyxSessions == nil || strings.TrimSpace(sessionKey) == "" {
		return OnyxSessionBinding{}
	}
	now := time.Now()
	current, _ := s.onyxSessions.Get(sessionKey)
	merged := mergeOnyxSessionBinding(current, update, now, s.onyxSessions.ttl)
	if s.onyxSharedStore != nil && accountID > 0 && strings.TrimSpace(sessionHash) != "" {
		cacheCtx, cancel := withOnyxSessionStoreRedisTimeout(ctx)
		err := s.onyxSharedStore.Set(cacheCtx, groupID, accountID, sessionHash, merged, normalizeWebOnyxTTL(s.onyxSessions.ttl))
		cancel()
		if err != nil {
			recordOnyxSessionSharedStoreSetError()
			recordOnyxSharedStoreDegradedWrite()
			s.logOnyxSessionError("onyx.shared_session_set_failed", "shared", &webOnyxPreparedRequest{
				GroupID:     groupID,
				AccountID:   accountID,
				SessionHash: sessionHash,
			}, merged, err)
		}
	}
	s.onyxSessions.Upsert(sessionKey, merged)
	return merged
}

func (s *WebGatewayService) deleteOnyxSessionBinding(ctx context.Context, prepared *webOnyxPreparedRequest) {
	if s == nil || prepared == nil || strings.TrimSpace(prepared.SessionKey) == "" {
		return
	}
	if s.onyxSharedStore != nil && prepared.AccountID > 0 && strings.TrimSpace(prepared.SessionHash) != "" {
		cacheCtx, cancel := withOnyxSessionStoreRedisTimeout(ctx)
		err := s.onyxSharedStore.Delete(cacheCtx, prepared.GroupID, prepared.AccountID, prepared.SessionHash)
		cancel()
		if err != nil {
			recordOnyxSessionSharedStoreDeleteError()
			s.logOnyxSessionError("onyx.shared_session_delete_failed", "recovered", prepared, OnyxSessionBinding{}, err)
		}
	}
	s.onyxSessions.Delete(prepared.SessionKey)
}

func (s *WebGatewayService) createOnyxChatSession(ctx context.Context, account *Account, creds webCredentials, sendTargetURL string, upstreamBody []byte, input webPromptInput) (string, error) {
	recordOnyxCreateChatSession()
	createURL, err := deriveOnyxCreateChatSessionURL(sendTargetURL)
	if err != nil {
		return "", err
	}

	payload := map[string]any{"persona_id": 0}
	if personaID := gjson.GetBytes(upstreamBody, "chat_session_info.persona_id"); personaID.Exists() && personaID.Type != gjson.Null {
		payload["persona_id"] = int(personaID.Int())
	}
	if description := strings.TrimSpace(gjson.GetBytes(upstreamBody, "chat_session_info.description").String()); description != "" {
		payload["description"] = description
	}
	if projectID := gjson.GetBytes(upstreamBody, "chat_session_info.project_id"); projectID.Exists() && projectID.Type != gjson.Null {
		payload["project_id"] = projectID.Value()
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal onyx create session payload: %w", err)
	}

	var lastErr error
	usedMethod := ""
	for _, method := range []string{http.MethodPut, http.MethodPost} {
		respBody, statusCode, err := s.doOnyxAuxJSONRequest(ctx, account, creds, method, createURL, body)
		if err != nil {
			lastErr = err
			continue
		}
		if statusCode < 200 || statusCode >= 300 {
			lastErr = fmt.Errorf("create onyx chat session failed: status %d", statusCode)
			continue
		}
		chatSessionID := strings.TrimSpace(gjson.GetBytes(respBody, "chat_session_id").String())
		if chatSessionID == "" {
			lastErr = fmt.Errorf("create onyx chat session missing chat_session_id")
			continue
		}
		sessionKey := s.onyxSessionKey(account, input)
		s.upsertOnyxSessionBinding(ctx, sessionKey, derefGroupID(input.GroupID), account.ID, strings.TrimSpace(input.SessionHash), OnyxSessionBinding{ChatSessionID: chatSessionID})
		recordOnyxCreateChatSessionSuccess()
		usedMethod = method
		s.logOnyxSessionDebug("onyx.create_chat_session_succeeded", "new", &webOnyxPreparedRequest{
			GroupID:       derefGroupID(input.GroupID),
			AccountID:     account.ID,
			SessionHash:   strings.TrimSpace(input.SessionHash),
			ChatSessionID: chatSessionID,
		}, OnyxSessionBinding{ChatSessionID: chatSessionID}, zap.String("method", usedMethod))
		return chatSessionID, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("create onyx chat session failed")
	}
	return "", lastErr
}

func (s *WebGatewayService) doOnyxAuxJSONRequest(ctx context.Context, account *Account, creds webCredentials, method, targetURL string, body []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("accept", "application/json")
	for k, v := range creds.Headers {
		if strings.TrimSpace(k) != "" {
			req.Header.Set(k, v)
		}
	}
	applyWebAuth(req, creds.Auth)

	proxyURL := ""
	if account != nil && account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if readErr != nil {
		return nil, resp.StatusCode, readErr
	}
	return respBody, resp.StatusCode, nil
}

func deriveOnyxCreateChatSessionURL(sendTargetURL string) (string, error) {
	u, err := url.Parse(sendTargetURL)
	if err != nil {
		return "", fmt.Errorf("parse onyx send-chat-message url: %w", err)
	}
	path := strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(path, "/chat/send-chat-message") {
		return "", fmt.Errorf("onyx session-aware mode requires /chat/send-chat-message base_url")
	}
	u.Path = strings.TrimSuffix(path, "/chat/send-chat-message") + "/chat/create-chat-session"
	u.RawQuery = ""
	return u.String(), nil
}

func deriveOnyxGetChatSessionURL(sendTargetURL, chatSessionID string) (string, error) {
	u, err := url.Parse(sendTargetURL)
	if err != nil {
		return "", fmt.Errorf("parse onyx send-chat-message url: %w", err)
	}
	path := strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(path, "/chat/send-chat-message") {
		return "", fmt.Errorf("onyx session-aware mode requires /chat/send-chat-message base_url")
	}
	u.Path = strings.TrimSuffix(path, "/chat/send-chat-message") + "/chat/get-chat-session/" + url.PathEscape(strings.TrimSpace(chatSessionID))
	u.RawQuery = ""
	return u.String(), nil
}

func (s *WebGatewayService) persistOnyxStreamingSession(ctx context.Context, prepared *webOnyxPreparedRequest, upstream webOnyxUpstreamSession) {
	if s == nil || prepared == nil || strings.TrimSpace(prepared.SessionKey) == "" {
		return
	}
	update := OnyxSessionBinding{
		ChatSessionID:              firstNonEmptyString(strings.TrimSpace(upstream.ChatSessionID), strings.TrimSpace(prepared.ChatSessionID)),
		ReservedAssistantMessageID: upstream.ReservedAssistantMessageID,
		UserMessageID:              upstream.UserMessageID,
	}
	if upstream.ReservedAssistantMessageID > 0 {
		update.ParentMessageID = upstream.ReservedAssistantMessageID
	} else if upstream.ParentMessageID > 0 {
		update.ParentMessageID = upstream.ParentMessageID
	}
	if update.ParentMessageID > 0 {
		recordOnyxStreamingParentAdvance()
	}
	s.upsertOnyxSessionBinding(ctx, prepared.SessionKey, prepared.GroupID, prepared.AccountID, prepared.SessionHash, update)
}

func (s *WebGatewayService) handleOnyxNonStreamingSessionState(ctx context.Context, account *Account, creds webCredentials, sendTargetURL string, prepared *webOnyxPreparedRequest, respBody []byte) {
	if s == nil || prepared == nil || strings.TrimSpace(prepared.SessionKey) == "" {
		return
	}
	chatSessionID := strings.TrimSpace(gjson.GetBytes(respBody, "chat_session_id").String())
	if chatSessionID == "" {
		chatSessionID = strings.TrimSpace(prepared.ChatSessionID)
	}
	if chatSessionID == "" {
		return
	}
	s.upsertOnyxSessionBinding(ctx, prepared.SessionKey, prepared.GroupID, prepared.AccountID, prepared.SessionHash, OnyxSessionBinding{ChatSessionID: chatSessionID})

	sessionURL, err := deriveOnyxGetChatSessionURL(sendTargetURL, chatSessionID)
	if err != nil {
		return
	}
	recordOnyxNonstreamParentRefresh()
	sessionBody, statusCode, err := s.doOnyxAuxJSONRequest(ctx, account, creds, http.MethodGet, sessionURL, nil)
	if err != nil || statusCode < 200 || statusCode >= 300 {
		return
	}
	latest := extractOnyxLatestAssistantBinding(sessionBody)
	if latest.ChatSessionID == "" {
		latest.ChatSessionID = chatSessionID
	}
	if latest.ParentMessageID > 0 {
		recordOnyxNonstreamParentRefreshSuccess()
		source := prepared.BindingSource
		if source == "" {
			source = "local"
		}
		s.logOnyxSessionDebug("onyx.nonstream_parent_refresh_succeeded", source, prepared, latest)
	}
	s.upsertOnyxSessionBinding(ctx, prepared.SessionKey, prepared.GroupID, prepared.AccountID, prepared.SessionHash, latest)
}

func extractOnyxLatestAssistantBinding(body []byte) OnyxSessionBinding {
	binding := OnyxSessionBinding{
		ChatSessionID: strings.TrimSpace(gjson.GetBytes(body, "chat_session_id").String()),
	}
	var bestMessageID int64
	messages := gjson.GetBytes(body, "messages")
	if !messages.Exists() || !messages.IsArray() {
		return binding
	}
	for _, message := range messages.Array() {
		if message.Get("message_type").String() != "assistant" {
			continue
		}
		if errVal := strings.TrimSpace(message.Get("error").String()); errVal != "" {
			continue
		}
		messageID := message.Get("message_id").Int()
		if messageID <= bestMessageID {
			continue
		}
		bestMessageID = messageID
		binding.ParentMessageID = messageID
	}
	return binding
}

func (s *WebGatewayService) shouldRetryOnyxInvalidSession(statusCode int, body []byte) bool {
	if statusCode != http.StatusBadRequest && statusCode != http.StatusNotFound {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(extractUpstreamErrorMessage(body)))
	if msg == "" {
		msg = strings.ToLower(strings.TrimSpace(string(body)))
	}
	return strings.Contains(msg, "invalid session") ||
		strings.Contains(msg, "chat session") ||
		strings.Contains(msg, "parent message") ||
		strings.Contains(msg, "parent_message_id") ||
		strings.Contains(msg, "message_id")
}

func (s *WebGatewayService) logOnyxSessionDebug(message, source string, prepared *webOnyxPreparedRequest, binding OnyxSessionBinding, extraFields ...zap.Field) {
	fields := s.onyxSessionLogFields(source, prepared, binding)
	fields = append(fields, extraFields...)
	logger.L().With(zap.String("component", "service.web_gateway")).Debug(message, fields...)
}

func (s *WebGatewayService) logOnyxSessionError(message, source string, prepared *webOnyxPreparedRequest, binding OnyxSessionBinding, err error, extraFields ...zap.Field) {
	fields := s.onyxSessionLogFields(source, prepared, binding)
	fields = append(fields, zap.Error(err))
	fields = append(fields, extraFields...)
	logger.L().With(zap.String("component", "service.web_gateway")).Warn(message, fields...)
}

func (s *WebGatewayService) onyxSessionLogFields(source string, prepared *webOnyxPreparedRequest, binding OnyxSessionBinding) []zap.Field {
	groupID := int64(0)
	accountID := int64(0)
	sessionHash := ""
	if prepared != nil {
		groupID = prepared.GroupID
		accountID = prepared.AccountID
		sessionHash = prepared.SessionHash
	}
	binding = normalizeOnyxSessionBinding(binding)
	chatSessionID := binding.ChatSessionID
	if chatSessionID == "" && prepared != nil {
		chatSessionID = prepared.ChatSessionID
	}
	parentMessageID := binding.ParentMessageID
	return []zap.Field{
		zap.Int64("group_id", groupID),
		zap.Int64("account_id", accountID),
		zap.String("session_hash_short", shortSessionHash(sessionHash)),
		zap.String("chat_session_id", shortOnyxSessionIdentifier(chatSessionID)),
		zap.Int64("parent_message_id", parentMessageID),
		zap.Int64("reserved_assistant_message_id", binding.ReservedAssistantMessageID),
		zap.String("source", source),
	}
}

func shortOnyxSessionIdentifier(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if len(raw) <= 12 {
		return raw
	}
	return raw[:12]
}

func mapWebRequestedModel(model string, account *Account, creds webCredentials) string {
	if len(creds.ModelMapping) == 0 {
		if account == nil {
			return model
		}
		return account.GetMappedModel(model)
	}
	tmp := Account{Credentials: map[string]any{"model_mapping": creds.ModelMapping}}
	return tmp.GetMappedModel(model)
}

func buildWebTemplateValues(input webPromptInput, upstreamModel string) webTemplateValues {
	prompt := webBuildPrompt(input.System, input.Messages, input.Body)
	body := input.Body
	values := webTemplateValues{
		Model:                 upstreamModel,
		Prompt:                prompt,
		MessagesJSON:          webMessagesJSON(input.Messages),
		System:                webTextFromAny(input.System),
		MetadataUserID:        input.MetadataUserID,
		StreamJSON:            strconv.FormatBool(input.Stream),
		MaxTokensJSON:         firstRawJSON(body, "max_tokens", "max_completion_tokens", "max_output_tokens"),
		TemperatureJSON:       firstRawJSON(body, "temperature"),
		TopPJSON:              firstRawJSON(body, "top_p"),
		StopJSON:              firstRawJSON(body, "stop", "stop_sequences"),
		ToolsJSON:             firstRawJSON(body, "tools", "functions"),
		ToolChoiceJSON:        firstRawJSON(body, "tool_choice", "function_call"),
		MetadataJSON:          firstRawJSON(body, "metadata"),
		RawRequestJSON:        string(body),
		AnthropicMessagesJSON: "[]",
		ChatMessagesJSON:      "[]",
		ResponsesInputJSON:    "[]",
	}
	values.ModelProvider, values.ModelVersion = splitWebModelProviderVersion(upstreamModel)
	if msgs := firstRawJSON(body, "messages"); msgs != "" {
		values.ChatMessagesJSON = msgs
		values.AnthropicMessagesJSON = msgs
	}
	if inputRaw := firstRawJSON(body, "input"); inputRaw != "" {
		values.ResponsesInputJSON = inputRaw
	}
	if req, err := webBuildAnthropicRequest(input, upstreamModel); err == nil && req != nil {
		if b, err := json.Marshal(req.Messages); err == nil {
			values.AnthropicMessagesJSON = string(b)
			values.MessagesJSON = string(b)
		}
		if len(req.System) > 0 {
			values.System = webTextFromRawJSON(req.System)
		}
		if len(req.Tools) > 0 {
			if b, err := json.Marshal(req.Tools); err == nil {
				values.ToolsJSON = string(b)
			}
		}
		if len(req.ToolChoice) > 0 {
			values.ToolChoiceJSON = string(req.ToolChoice)
		}
		if req.MaxTokens > 0 {
			values.MaxTokensJSON = strconv.Itoa(req.MaxTokens)
		}
	}
	return values
}

func webBuildAnthropicRequest(input webPromptInput, upstreamModel string) (*apicompat.AnthropicRequest, error) {
	switch input.EndpointFormat {
	case webForwardChatCompletions:
		var ccReq apicompat.ChatCompletionsRequest
		if err := json.Unmarshal(input.Body, &ccReq); err != nil {
			return nil, err
		}
		ccReq.Model = upstreamModel
		responsesReq, err := apicompat.ChatCompletionsToResponses(&ccReq)
		if err != nil {
			return nil, err
		}
		anthReq, err := apicompat.ResponsesToAnthropicRequest(responsesReq)
		if err != nil {
			return nil, err
		}
		anthReq.Model = upstreamModel
		anthReq.Stream = input.Stream
		return anthReq, nil
	case webForwardResponses:
		var respReq apicompat.ResponsesRequest
		if err := json.Unmarshal(input.Body, &respReq); err != nil {
			return nil, err
		}
		respReq.Model = upstreamModel
		anthReq, err := apicompat.ResponsesToAnthropicRequest(&respReq)
		if err != nil {
			return nil, err
		}
		anthReq.Model = upstreamModel
		anthReq.Stream = input.Stream
		return anthReq, nil
	default:
		var anthReq apicompat.AnthropicRequest
		if err := json.Unmarshal(input.Body, &anthReq); err != nil {
			return nil, err
		}
		anthReq.Model = upstreamModel
		anthReq.Stream = input.Stream
		return &anthReq, nil
	}
}

func firstRawJSON(body []byte, paths ...string) string {
	for _, path := range paths {
		res := gjson.GetBytes(body, path)
		if res.Exists() {
			if res.Raw != "" {
				return res.Raw
			}
			if res.Type == gjson.String {
				b, _ := json.Marshal(res.String())
				return string(b)
			}
		}
	}
	return ""
}

func webTextFromRawJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return webTextFromAny(v)
}

func (s *WebGatewayService) handleWebUpstreamError(ctx context.Context, c *gin.Context, account *Account, statusCode int, headers http.Header, body []byte, targetURL string) error {
	upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(body))
	if upstreamMsg == "" {
		upstreamMsg = http.StatusText(statusCode)
	}
	upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)
	upstreamMsg = withWebSessionDiagnostic(statusCode, headers, upstreamMsg)
	setOpsUpstreamError(c, statusCode, upstreamMsg, "")
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		Platform:           account.Platform,
		AccountID:          account.ID,
		AccountName:        account.Name,
		UpstreamStatusCode: statusCode,
		UpstreamRequestID:  headers.Get("x-request-id"),
		UpstreamURL:        safeUpstreamURL(targetURL),
		Kind:               "failover",
		Message:            upstreamMsg,
	})
	if s.rateLimitService != nil {
		s.rateLimitService.HandleUpstreamError(ctx, account, statusCode, headers, body)
	}
	return &UpstreamFailoverError{
		StatusCode:      statusCode,
		ResponseBody:    body,
		ResponseHeaders: headers,
	}
}

func withWebSessionDiagnostic(statusCode int, headers http.Header, upstreamMsg string) string {
	switch statusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "web session rejected: check whether the cookie/session expired, CSRF/header profile is missing, or the upstream binds session to device/IP; upstream: " + upstreamMsg
	case http.StatusTooManyRequests:
		if retryAfter := strings.TrimSpace(headers.Get("Retry-After")); retryAfter != "" {
			return "upstream rate limited; retry-after=" + retryAfter + "; upstream: " + upstreamMsg
		}
		return "upstream rate limited; upstream: " + upstreamMsg
	default:
		if statusCode >= 500 {
			return "upstream server error; failover may retry another account before streaming starts; upstream: " + upstreamMsg
		}
		return upstreamMsg
	}
}

func (s *WebGatewayService) ensureFreshWebToken(ctx context.Context, account *Account) error {
	if s == nil || s.oauthService == nil || account == nil || account.Platform == PlatformWeb {
		return nil
	}
	return s.oauthService.EnsureFreshAccessToken(ctx, account, time.Minute)
}

func (s *WebGatewayService) refreshWebAccountAfterAuthError(ctx context.Context, account *Account) error {
	if s == nil || s.oauthService == nil || account == nil || account.Platform == PlatformWeb {
		return fmt.Errorf("web oauth refresh is not configured")
	}
	result, err := s.oauthService.RefreshAccount(ctx, account.Platform, account.ID)
	if err != nil {
		return err
	}
	if result != nil && result.Credentials != nil {
		account.Credentials = result.Credentials
	}
	return nil
}

type webTemplateValues struct {
	Model                 string
	ModelProvider         string
	ModelVersion          string
	Prompt                string
	MessagesJSON          string
	System                string
	MetadataUserID        string
	StreamJSON            string
	MaxTokensJSON         string
	TemperatureJSON       string
	TopPJSON              string
	StopJSON              string
	ToolsJSON             string
	ToolChoiceJSON        string
	MetadataJSON          string
	RawRequestJSON        string
	AnthropicMessagesJSON string
	ChatMessagesJSON      string
	ResponsesInputJSON    string
}

func buildWebRequestBody(template string, values webTemplateValues) ([]byte, error) {
	body := template
	body = strings.ReplaceAll(body, "{{model}}", escapeJSONStringContent(values.Model))
	body = strings.ReplaceAll(body, "{{model_provider}}", escapeJSONStringContent(values.ModelProvider))
	body = strings.ReplaceAll(body, "{{model_version}}", escapeJSONStringContent(values.ModelVersion))
	body = strings.ReplaceAll(body, "{{prompt}}", escapeJSONStringContent(values.Prompt))
	body = strings.ReplaceAll(body, "{{system}}", escapeJSONStringContent(values.System))
	body = strings.ReplaceAll(body, "{{metadata_user_id}}", escapeJSONStringContent(values.MetadataUserID))
	body = strings.ReplaceAll(body, "{{stream}}", jsonOrDefault(values.StreamJSON, "false"))
	body = strings.ReplaceAll(body, "{{max_tokens}}", jsonOrDefault(values.MaxTokensJSON, "null"))
	body = strings.ReplaceAll(body, "{{temperature}}", jsonOrDefault(values.TemperatureJSON, "null"))
	body = strings.ReplaceAll(body, "{{top_p}}", jsonOrDefault(values.TopPJSON, "null"))
	body = strings.ReplaceAll(body, "{{stop_json}}", jsonOrDefault(values.StopJSON, "null"))
	body = strings.ReplaceAll(body, "{{tools_json}}", jsonOrDefault(values.ToolsJSON, "null"))
	body = strings.ReplaceAll(body, "{{tool_choice_json}}", jsonOrDefault(values.ToolChoiceJSON, "null"))
	body = strings.ReplaceAll(body, "{{metadata_json}}", jsonOrDefault(values.MetadataJSON, "null"))
	body = strings.ReplaceAll(body, "{{raw_request_json}}", jsonOrDefault(values.RawRequestJSON, "{}"))
	body = strings.ReplaceAll(body, "{{messages_json}}", jsonOrDefault(values.MessagesJSON, "[]"))
	body = strings.ReplaceAll(body, "{{anthropic_messages_json}}", jsonOrDefault(values.AnthropicMessagesJSON, "[]"))
	body = strings.ReplaceAll(body, "{{chat_messages_json}}", jsonOrDefault(values.ChatMessagesJSON, "[]"))
	body = strings.ReplaceAll(body, "{{responses_input_json}}", jsonOrDefault(values.ResponsesInputJSON, "[]"))
	if !gjson.Valid(body) {
		return nil, fmt.Errorf("web request_template produced invalid JSON")
	}
	return []byte(body), nil
}

func splitWebModelProviderVersion(model string) (provider, version string) {
	model = strings.TrimSpace(model)
	if model == "" {
		return "", ""
	}
	for _, sep := range []string{"/", ":"} {
		if idx := strings.Index(model, sep); idx > 0 && idx < len(model)-1 {
			return strings.TrimSpace(model[:idx]), strings.TrimSpace(model[idx+1:])
		}
	}
	lower := strings.ToLower(model)
	switch {
	case strings.Contains(lower, "gpt") || strings.Contains(lower, "openai"):
		return "openai", model
	case strings.Contains(lower, "claude") || strings.Contains(lower, "opus") || strings.Contains(lower, "sonnet") || strings.Contains(lower, "haiku"):
		return "anthropic", model
	default:
		return "", model
	}
}

func jsonOrDefault(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	if value == "null" || value == "true" || value == "false" || gjson.Valid(value) {
		return value
	}
	return fallback
}

func escapeJSONStringContent(value string) string {
	encoded, _ := json.Marshal(value)
	if len(encoded) < 2 {
		return ""
	}
	return string(encoded[1 : len(encoded)-1])
}

func applyWebAuth(req *http.Request, auth webAuthConfig) {
	switch strings.ToLower(strings.TrimSpace(auth.Type)) {
	case "bearer":
		if token := strings.TrimSpace(auth.Token); token != "" {
			req.Header.Set("authorization", "Bearer "+token)
		}
	case "header":
		header := strings.TrimSpace(auth.Header)
		if header == "" {
			header = "x-api-key"
		}
		if token := strings.TrimSpace(auth.Token); token != "" {
			req.Header.Set(header, token)
		}
	}
}

func parseWebResponse(body []byte, cfg webResponseConfig) (text, requestID, model string, usage ClaudeUsage, err error) {
	if !gjson.ValidBytes(body) {
		return "", "", "", usage, fmt.Errorf("web upstream response is not valid JSON")
	}
	textResult := gjson.GetBytes(body, cfg.TextPath)
	if !textResult.Exists() {
		return "", "", "", usage, fmt.Errorf("web upstream response missing text_path %q", cfg.TextPath)
	}
	text = textResult.String()
	if cfg.RequestIDPath != "" {
		requestID = strings.TrimSpace(gjson.GetBytes(body, cfg.RequestIDPath).String())
	}
	if cfg.ModelPath != "" {
		model = strings.TrimSpace(gjson.GetBytes(body, cfg.ModelPath).String())
	}
	usage = ClaudeUsage{
		InputTokens:              webIntPath(body, cfg.UsagePaths.InputTokens),
		OutputTokens:             webIntPath(body, cfg.UsagePaths.OutputTokens),
		CacheCreationInputTokens: webIntPath(body, cfg.UsagePaths.CacheCreationInputTokens),
		CacheReadInputTokens:     webIntPath(body, cfg.UsagePaths.CacheReadInputTokens),
	}
	return text, requestID, model, usage, nil
}

func parseWebResponseToAnthropic(body []byte, creds webCredentials, requestedModel, upstreamModel string) (*apicompat.AnthropicResponse, string, string, ClaudeUsage, error) {
	if !gjson.ValidBytes(body) {
		return nil, "", "", ClaudeUsage{}, fmt.Errorf("web upstream response is not valid JSON")
	}
	switch creds.UpstreamFormat {
	case "anthropic_messages":
		var resp apicompat.AnthropicResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, "", "", ClaudeUsage{}, fmt.Errorf("parse anthropic web response: %w", err)
		}
		if resp.ID == "" {
			resp.ID = strings.TrimSpace(gjson.GetBytes(body, creds.Response.RequestIDPath).String())
		}
		if resp.Model == "" {
			resp.Model = requestedModel
		}
		usage := claudeUsageFromAnthropic(resp.Usage)
		return &resp, resp.ID, resp.Model, usage, nil
	case "openai_responses":
		var resp apicompat.ResponsesResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, "", "", ClaudeUsage{}, fmt.Errorf("parse responses web response: %w", err)
		}
		anth := apicompat.ResponsesToAnthropic(&resp, requestedModel)
		usage := ClaudeUsage{}
		if resp.Usage != nil {
			usage.InputTokens = resp.Usage.InputTokens
			usage.OutputTokens = resp.Usage.OutputTokens
			if resp.Usage.InputTokensDetails != nil {
				usage.CacheReadInputTokens = resp.Usage.InputTokensDetails.CachedTokens
			}
		}
		model := strings.TrimSpace(resp.Model)
		return anth, resp.ID, model, usage, nil
	case "openai_chat":
		var resp apicompat.ChatCompletionsResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, "", "", ClaudeUsage{}, fmt.Errorf("parse chat completions web response: %w", err)
		}
		anth := chatCompletionsResponseToAnthropic(resp, requestedModel)
		usage := ClaudeUsage{}
		if resp.Usage != nil {
			usage.InputTokens = resp.Usage.PromptTokens
			usage.OutputTokens = resp.Usage.CompletionTokens
			if resp.Usage.PromptTokensDetails != nil {
				usage.CacheReadInputTokens = resp.Usage.PromptTokensDetails.CachedTokens
			}
		}
		return anth, resp.ID, strings.TrimSpace(resp.Model), usage, nil
	default:
		return parseConfiguredWebResponseToAnthropic(body, creds.Response, requestedModel, upstreamModel)
	}
}

func chatCompletionsResponseToAnthropic(resp apicompat.ChatCompletionsResponse, requestedModel string) *apicompat.AnthropicResponse {
	model := requestedModel
	if model == "" {
		model = resp.Model
	}
	content := []apicompat.AnthropicContentBlock{}
	stopReason := "end_turn"
	if len(resp.Choices) > 0 {
		choice := resp.Choices[0]
		text := webTextFromRawJSON(choice.Message.Content)
		if text != "" {
			content = append(content, apicompat.AnthropicContentBlock{Type: "text", Text: text})
		}
		for _, tc := range choice.Message.ToolCalls {
			id := strings.TrimSpace(tc.ID)
			if id == "" {
				id = "toolu_" + uuid.NewString()
			}
			args := strings.TrimSpace(tc.Function.Arguments)
			if args == "" || !gjson.Valid(args) {
				args = "{}"
			}
			content = append(content, apicompat.AnthropicContentBlock{
				Type:  "tool_use",
				ID:    id,
				Name:  strings.TrimSpace(tc.Function.Name),
				Input: json.RawMessage(args),
			})
		}
		stopReason = normalizeWebStopReason(choice.FinishReason, len(choice.Message.ToolCalls) > 0)
	}
	if len(content) == 0 {
		content = append(content, apicompat.AnthropicContentBlock{Type: "text", Text: ""})
	}
	usage := apicompat.AnthropicUsage{}
	if resp.Usage != nil {
		usage.InputTokens = resp.Usage.PromptTokens
		usage.OutputTokens = resp.Usage.CompletionTokens
		if resp.Usage.PromptTokensDetails != nil {
			usage.CacheReadInputTokens = resp.Usage.PromptTokensDetails.CachedTokens
		}
	}
	return &apicompat.AnthropicResponse{
		ID:         resp.ID,
		Type:       "message",
		Role:       "assistant",
		Content:    content,
		Model:      model,
		StopReason: stopReason,
		Usage:      usage,
	}
}

func parseConfiguredWebResponseToAnthropic(body []byte, cfg webResponseConfig, requestedModel, upstreamModel string) (*apicompat.AnthropicResponse, string, string, ClaudeUsage, error) {
	usage := ClaudeUsage{
		InputTokens:              webIntPath(body, cfg.UsagePaths.InputTokens),
		OutputTokens:             webIntPath(body, cfg.UsagePaths.OutputTokens),
		CacheCreationInputTokens: webIntPath(body, cfg.UsagePaths.CacheCreationInputTokens),
		CacheReadInputTokens:     webIntPath(body, cfg.UsagePaths.CacheReadInputTokens),
	}
	requestID := ""
	if cfg.RequestIDPath != "" {
		requestID = strings.TrimSpace(gjson.GetBytes(body, cfg.RequestIDPath).String())
	}
	model := ""
	if cfg.ModelPath != "" {
		model = strings.TrimSpace(gjson.GetBytes(body, cfg.ModelPath).String())
	}
	if model == "" {
		model = upstreamModel
	}
	if model == "" {
		model = requestedModel
	}

	stopReason := strings.TrimSpace(gjson.GetBytes(body, cfg.StopReasonPath).String())
	stopReason = normalizeWebStopReason(stopReason, false)
	content := []apicompat.AnthropicContentBlock{}
	text := ""
	if strings.TrimSpace(cfg.TextPath) != "" {
		textResult := gjson.GetBytes(body, cfg.TextPath)
		if !textResult.Exists() && strings.TrimSpace(cfg.ToolCalls.Path) == "" {
			return nil, "", "", usage, fmt.Errorf("web upstream response missing text_path %q", cfg.TextPath)
		}
		if textResult.Exists() {
			text = textResult.String()
		}
	}
	if text != "" {
		content = append(content, apicompat.AnthropicContentBlock{Type: "text", Text: text})
	}
	toolUses := parseWebToolUses(body, cfg.ToolCalls)
	if len(toolUses) > 0 {
		content = append(content, toolUses...)
		stopReason = "tool_use"
	}
	if len(content) == 0 {
		content = append(content, apicompat.AnthropicContentBlock{Type: "text", Text: ""})
	}
	if stopReason == "" {
		stopReason = "end_turn"
	}
	resp := &apicompat.AnthropicResponse{
		ID:         requestID,
		Type:       "message",
		Role:       "assistant",
		Content:    content,
		Model:      requestedModel,
		StopReason: stopReason,
		Usage:      anthropicUsageFromClaude(usage),
	}
	return resp, requestID, model, usage, nil
}

func parseWebToolUses(body []byte, cfg webToolCallConfig) []apicompat.AnthropicContentBlock {
	if strings.TrimSpace(cfg.Path) == "" {
		return nil
	}
	arr := gjson.GetBytes(body, cfg.Path)
	if !arr.Exists() {
		return nil
	}
	results := arr.Array()
	if !arr.IsArray() {
		results = []gjson.Result{arr}
	}
	var out []apicompat.AnthropicContentBlock
	for idx, item := range results {
		id := strings.TrimSpace(item.Get(pathOrDefaultString(cfg.IDPath, "id")).String())
		if id == "" {
			id = "toolu_" + uuid.NewString()
		}
		name := strings.TrimSpace(item.Get(pathOrDefaultString(cfg.NamePath, "name")).String())
		if name == "" {
			name = strings.TrimSpace(item.Get("function.name").String())
		}
		if name == "" {
			name = fmt.Sprintf("tool_%d", idx+1)
		}
		argsRaw := "{}"
		argPath := pathOrDefaultString(cfg.ArgumentsPath, "arguments")
		argRes := item.Get(argPath)
		if argRes.Exists() {
			if strings.EqualFold(cfg.ArgumentsFormat, "object") {
				argsRaw = argRes.Raw
			} else {
				argsRaw = argRes.String()
			}
		}
		argsRaw = strings.TrimSpace(argsRaw)
		if argsRaw == "" || !gjson.Valid(argsRaw) {
			argsRaw = "{}"
		}
		out = append(out, apicompat.AnthropicContentBlock{
			Type:  "tool_use",
			ID:    id,
			Name:  name,
			Input: json.RawMessage(argsRaw),
		})
	}
	return out
}

func pathOrDefaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func normalizeWebStopReason(reason string, sawTool bool) string {
	reason = strings.ToLower(strings.TrimSpace(reason))
	if sawTool {
		return "tool_use"
	}
	switch reason {
	case "", "stop", "end_turn", "completed", "complete", "finish", "finished":
		return "end_turn"
	case "tool_calls", "tool_call", "function_call", "function_calls", "tool_use":
		return "tool_use"
	case "length", "max_tokens", "max_output_tokens":
		return "max_tokens"
	case "content_filter":
		return "stop_sequence"
	default:
		return reason
	}
}

func claudeUsageFromAnthropic(usage apicompat.AnthropicUsage) ClaudeUsage {
	return ClaudeUsage{
		InputTokens:              usage.InputTokens,
		OutputTokens:             usage.OutputTokens,
		CacheCreationInputTokens: usage.CacheCreationInputTokens,
		CacheReadInputTokens:     usage.CacheReadInputTokens,
	}
}

func anthropicUsageFromClaude(usage ClaudeUsage) apicompat.AnthropicUsage {
	return apicompat.AnthropicUsage{
		InputTokens:              usage.InputTokens,
		OutputTokens:             usage.OutputTokens,
		CacheCreationInputTokens: usage.CacheCreationInputTokens,
		CacheReadInputTokens:     usage.CacheReadInputTokens,
	}
}

func webTextFromAnthropicContent(blocks []apicompat.AnthropicContentBlock) string {
	var parts []string
	for _, block := range blocks {
		if block.Type == "text" && block.Text != "" {
			parts = append(parts, block.Text)
		}
	}
	return strings.Join(parts, "")
}

func webIntPath(body []byte, path string) int {
	if strings.TrimSpace(path) == "" {
		return 0
	}
	res := gjson.GetBytes(body, path)
	if !res.Exists() {
		return 0
	}
	return int(res.Int())
}

func writeWebSuccess(c *gin.Context, format webForwardFormat, out *webForwardOutput) {
	switch format {
	case webForwardChatCompletions:
		c.JSON(http.StatusOK, gin.H{
			"id":      out.RequestID,
			"object":  "chat.completion",
			"created": time.Now().Unix(),
			"model":   out.Model,
			"choices": []gin.H{{
				"index": 0,
				"message": gin.H{
					"role":    "assistant",
					"content": out.Text,
				},
				"finish_reason": "stop",
			}},
			"usage": webChatCompletionsUsage(out.Usage),
		})
	case webForwardResponses:
		c.JSON(http.StatusOK, gin.H{
			"id":      out.RequestID,
			"object":  "response",
			"created": time.Now().Unix(),
			"model":   out.Model,
			"output": []gin.H{{
				"type": "message",
				"role": "assistant",
				"content": []gin.H{{
					"type": "output_text",
					"text": out.Text,
				}},
			}},
			"usage": webOpenAIUsage(out.Usage),
		})
	default:
		c.JSON(http.StatusOK, gin.H{
			"id":            out.RequestID,
			"type":          "message",
			"role":          "assistant",
			"model":         out.Model,
			"content":       []gin.H{{"type": "text", "text": out.Text}},
			"stop_reason":   "end_turn",
			"stop_sequence": nil,
			"usage": gin.H{
				"input_tokens":                out.Usage.InputTokens,
				"output_tokens":               out.Usage.OutputTokens,
				"cache_creation_input_tokens": out.Usage.CacheCreationInputTokens,
				"cache_read_input_tokens":     out.Usage.CacheReadInputTokens,
			},
		})
	}
}

func writeWebAnthropicSuccess(c *gin.Context, format webForwardFormat, resp *apicompat.AnthropicResponse) {
	switch format {
	case webForwardChatCompletions:
		responsesResp := apicompat.AnthropicToResponsesResponse(resp)
		c.JSON(http.StatusOK, apicompat.ResponsesToChatCompletions(responsesResp, resp.Model))
	case webForwardResponses:
		c.JSON(http.StatusOK, apicompat.AnthropicToResponsesResponse(resp))
	default:
		c.JSON(http.StatusOK, resp)
	}
}

type webAnthropicStreamWriter struct {
	c              *gin.Context
	format         webForwardFormat
	model          string
	requestID      string
	started        bool
	stopped        bool
	usage          ClaudeUsage
	anthToResp     *apicompat.AnthropicEventToResponsesState
	respToChat     *apicompat.ResponsesEventToChatState
	includeUsage   bool
	headersWritten bool
	text           strings.Builder
}

func newWebAnthropicStreamWriter(c *gin.Context, format webForwardFormat, model, requestID string, includeUsage bool) *webAnthropicStreamWriter {
	w := &webAnthropicStreamWriter{
		c:            c,
		format:       format,
		model:        model,
		requestID:    requestID,
		anthToResp:   apicompat.NewAnthropicEventToResponsesState(),
		respToChat:   apicompat.NewResponsesEventToChatState(),
		includeUsage: includeUsage,
	}
	w.anthToResp.Model = model
	w.respToChat.Model = model
	w.respToChat.IncludeUsage = includeUsage
	return w
}

func (w *webAnthropicStreamWriter) writeEvents(events []apicompat.AnthropicStreamEvent) error {
	for i := range events {
		evt := events[i]
		if evt.Type == "message_start" {
			w.started = true
			if evt.Message != nil {
				if evt.Message.ID == "" {
					evt.Message.ID = w.requestID
				}
				if evt.Message.Model == "" {
					evt.Message.Model = w.model
				}
				mergeClaudeUsage(&w.usage, claudeUsageFromAnthropic(evt.Message.Usage))
			}
		}
		if evt.Type == "message_delta" && evt.Usage != nil {
			mergeClaudeUsage(&w.usage, claudeUsageFromAnthropic(*evt.Usage))
		}
		if evt.Type == "content_block_delta" && evt.Delta != nil && evt.Delta.Text != "" {
			w.text.WriteString(evt.Delta.Text)
		}
		if evt.Type == "message_stop" {
			w.stopped = true
		}
		if err := w.writeEvent(evt); err != nil {
			return err
		}
	}
	return nil
}

func (w *webAnthropicStreamWriter) writeEvent(evt apicompat.AnthropicStreamEvent) error {
	w.ensureHeaders()
	switch w.format {
	case webForwardChatCompletions:
		respEvents := apicompat.AnthropicEventToResponsesEvents(&evt, w.anthToResp)
		for i := range respEvents {
			chunks := apicompat.ResponsesEventToChatChunks(&respEvents[i], w.respToChat)
			for j := range chunks {
				line, err := apicompat.ChatChunkToSSE(chunks[j])
				if err != nil {
					return err
				}
				if err := writeWebSSEString(w.c, line); err != nil {
					return err
				}
			}
		}
	case webForwardResponses:
		respEvents := apicompat.AnthropicEventToResponsesEvents(&evt, w.anthToResp)
		for i := range respEvents {
			line, err := apicompat.ResponsesEventToSSE(respEvents[i])
			if err != nil {
				return err
			}
			if err := writeWebSSEString(w.c, line); err != nil {
				return err
			}
		}
	default:
		line, err := apicompat.ResponsesAnthropicEventToSSE(evt)
		if err != nil {
			return err
		}
		if err := writeWebSSEString(w.c, line); err != nil {
			return err
		}
	}
	return nil
}

func (w *webAnthropicStreamWriter) finalize() error {
	if !w.started || w.stopped {
		return nil
	}
	events := []apicompat.AnthropicStreamEvent{
		{
			Type: "message_delta",
			Delta: &apicompat.AnthropicDelta{
				StopReason:   "end_turn",
				StopSequence: nil,
			},
			Usage: ptrAnthropicUsage(anthropicUsageFromClaude(w.usage)),
		},
		{Type: "message_stop"},
	}
	return w.writeEvents(events)
}

func (w *webAnthropicStreamWriter) ensureHeaders() {
	if w.headersWritten {
		return
	}
	w.headersWritten = true
	w.c.Writer.Header().Set("Content-Type", "text/event-stream")
	w.c.Writer.Header().Set("Cache-Control", "no-cache")
	w.c.Writer.Header().Set("Connection", "keep-alive")
	w.c.Writer.Header().Set("X-Accel-Buffering", "no")
	w.c.Writer.WriteHeader(http.StatusOK)
}

func writeWebSSEString(c *gin.Context, line string) error {
	if _, err := c.Writer.WriteString(line); err != nil {
		return err
	}
	if flusher, ok := c.Writer.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

func ptrAnthropicUsage(v apicompat.AnthropicUsage) *apicompat.AnthropicUsage {
	return &v
}

func mergeClaudeUsage(dst *ClaudeUsage, src ClaudeUsage) {
	if dst == nil {
		return
	}
	if src.InputTokens > 0 {
		dst.InputTokens = src.InputTokens
	}
	if src.OutputTokens > 0 {
		dst.OutputTokens = src.OutputTokens
	}
	if src.CacheCreationInputTokens > 0 {
		dst.CacheCreationInputTokens = src.CacheCreationInputTokens
	}
	if src.CacheReadInputTokens > 0 {
		dst.CacheReadInputTokens = src.CacheReadInputTokens
	}
}

func estimateWebUsageIfMissing(usage ClaudeUsage, input webPromptInput, outputText string) (ClaudeUsage, bool) {
	if usage.InputTokens > 0 || usage.OutputTokens > 0 ||
		usage.CacheCreationInputTokens > 0 || usage.CacheReadInputTokens > 0 ||
		usage.CacheCreation5mTokens > 0 || usage.CacheCreation1hTokens > 0 ||
		usage.ImageOutputTokens > 0 {
		return usage, false
	}
	promptText := webBuildPrompt(input.System, input.Messages, input.Body)
	usage.InputTokens = estimateWebTokenCount(promptText)
	usage.OutputTokens = estimateWebTokenCount(outputText)
	return usage, true
}

func estimateWebTokenCount(text string) int {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	tokens := (len([]rune(text)) + 3) / 4
	if tokens < 1 {
		return 1
	}
	return tokens
}

func (s *WebGatewayService) handleWebStreamingResponse(resp *http.Response, c *gin.Context, creds webCredentials, format webForwardFormat, input webPromptInput, upstreamModel string, start time.Time, onyxPrepared *webOnyxPreparedRequest) (*webForwardOutput, error) {
	requestID := resp.Header.Get("x-request-id")
	if requestID == "" {
		requestID = "web_" + uuid.NewString()
	}
	writer := newWebAnthropicStreamWriter(c, format, input.Model, requestID, webChatIncludeUsage(input.Body))
	streamFormat := normalizeWebStreamFormat(creds.Response.Stream.Format, creds.UpstreamFormat)
	var err error
	var onyxState *webOnyxStreamState
	switch streamFormat {
	case "anthropic_sse":
		err = s.handleWebAnthropicSSE(resp.Body, writer)
	case "openai_chat_sse":
		err = s.handleWebOpenAIChatSSE(resp.Body, writer)
	case "openai_responses_sse":
		err = s.handleWebOpenAIResponsesSSE(resp.Body, writer)
	case "onyx_chat_sse":
		onyxState, err = s.handleWebOnyxChatSSE(resp.Body, writer, onyxPrepared)
	default:
		err = s.handleWebCustomTextSSE(resp.Body, writer, creds.Response)
	}
	if err != nil {
		if !writer.started {
			setOpsUpstreamError(c, http.StatusBadGateway, err.Error(), "")
			return nil, &UpstreamFailoverError{
				StatusCode:      http.StatusBadGateway,
				ResponseBody:    []byte(err.Error()),
				ResponseHeaders: resp.Header,
			}
		}
		return nil, err
	}
	if !writer.started {
		err := fmt.Errorf("web upstream stream ended without any parseable event")
		setOpsUpstreamError(c, http.StatusBadGateway, err.Error(), "")
		return nil, &UpstreamFailoverError{
			StatusCode:      http.StatusBadGateway,
			ResponseBody:    []byte(err.Error()),
			ResponseHeaders: resp.Header,
		}
	}
	if err := writer.finalize(); err != nil {
		return nil, err
	}
	if onyxPrepared != nil && onyxState != nil {
		s.persistOnyxStreamingSession(c.Request.Context(), onyxPrepared, onyxState.upstreamSession)
	}
	usage, usageEstimated := estimateWebUsageIfMissing(writer.usage, input, writer.text.String())
	return &webForwardOutput{
		RequestID:      requestID,
		Model:          input.Model,
		UpstreamModel:  optionalUpstreamModel(input.Model, upstreamModel),
		Usage:          usage,
		UsageEstimated: usageEstimated,
		Duration:       time.Since(start),
		Stream:         true,
	}, nil
}

func (s *WebGatewayService) handleWebAnthropicSSE(r io.Reader, writer *webAnthropicStreamWriter) error {
	return scanWebSSE(r, s.maxWebStreamLineSize(), func(eventName, payload string) error {
		if payload == "" || payload == "[DONE]" {
			return nil
		}
		var evt apicompat.AnthropicStreamEvent
		if err := json.Unmarshal([]byte(payload), &evt); err != nil {
			return nil
		}
		if evt.Type == "" {
			evt.Type = strings.TrimSpace(eventName)
		}
		if evt.Type == "" {
			return nil
		}
		return writer.writeEvents([]apicompat.AnthropicStreamEvent{evt})
	})
}

func (s *WebGatewayService) handleWebOpenAIResponsesSSE(r io.Reader, writer *webAnthropicStreamWriter) error {
	state := apicompat.NewResponsesEventToAnthropicState()
	state.Model = writer.model
	err := scanWebSSE(r, s.maxWebStreamLineSize(), func(_ string, payload string) error {
		if payload == "" || payload == "[DONE]" {
			return nil
		}
		var evt apicompat.ResponsesStreamEvent
		if err := json.Unmarshal([]byte(payload), &evt); err != nil {
			return nil
		}
		events := apicompat.ResponsesEventToAnthropicEvents(&evt, state)
		return writer.writeEvents(events)
	})
	if err != nil {
		return err
	}
	return writer.writeEvents(apicompat.FinalizeResponsesAnthropicStream(state))
}

func (s *WebGatewayService) handleWebOpenAIChatSSE(r io.Reader, writer *webAnthropicStreamWriter) error {
	state := newWebChatToAnthropicStreamState(writer.model)
	err := scanWebSSE(r, s.maxWebStreamLineSize(), func(_ string, payload string) error {
		if payload == "" || payload == "[DONE]" {
			return nil
		}
		var chunk apicompat.ChatCompletionsChunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			return nil
		}
		events := state.process(chunk)
		return writer.writeEvents(events)
	})
	if err != nil {
		return err
	}
	return writer.writeEvents(state.finalize())
}

func (s *WebGatewayService) handleWebCustomTextSSE(r io.Reader, writer *webAnthropicStreamWriter, cfg webResponseConfig) error {
	state := newWebTextToAnthropicStreamState(writer.model)
	doneMarker := strings.TrimSpace(cfg.Stream.DoneMarker)
	if doneMarker == "" {
		doneMarker = "[DONE]"
	}
	textPath := strings.TrimSpace(cfg.Stream.TextPath)
	if textPath == "" {
		textPath = cfg.TextPath
	}
	err := scanWebSSE(r, s.maxWebStreamLineSize(), func(_ string, payload string) error {
		payload = strings.TrimSpace(payload)
		if payload == "" || payload == doneMarker {
			return nil
		}
		text := payload
		if gjson.Valid(payload) && strings.TrimSpace(textPath) != "" {
			res := gjson.Get(payload, textPath)
			if !res.Exists() {
				return nil
			}
			text = res.String()
		}
		if text == "" {
			return nil
		}
		return writer.writeEvents(state.delta(text))
	})
	if err != nil {
		return err
	}
	return writer.writeEvents(state.finalize())
}

func (s *WebGatewayService) handleWebOnyxChatSSE(r io.Reader, writer *webAnthropicStreamWriter, prepared *webOnyxPreparedRequest) (*webOnyxStreamState, error) {
	state := newWebOnyxStreamState(writer.model)
	if prepared != nil {
		state.upstreamSession.ChatSessionID = prepared.ChatSessionID
	}
	err := scanWebSSE(r, s.maxWebStreamLineSize(), func(_ string, payload string) error {
		payload = strings.TrimSpace(payload)
		if payload == "" || payload == "[DONE]" {
			return nil
		}
		if !gjson.Valid(payload) {
			return nil
		}
		return handleWebOnyxPacketPayload(payload, state, writer)
	})
	if err != nil {
		return state, err
	}
	return state, writer.writeEvents(state.finalize())
}

func handleWebOnyxPacketPayload(payload string, state *webOnyxStreamState, writer *webAnthropicStreamWriter) error {
	res := gjson.Parse(payload)
	if res.IsArray() {
		for _, item := range res.Array() {
			if err := handleWebOnyxPacket(item, state, writer); err != nil {
				return err
			}
		}
		return nil
	}
	return handleWebOnyxPacket(res, state, writer)
}

func handleWebOnyxPacket(packet gjson.Result, state *webOnyxStreamState, writer *webAnthropicStreamWriter) error {
	if !packet.Exists() {
		return nil
	}
	if chatSessionID := strings.TrimSpace(packet.Get("chat_session_id").String()); chatSessionID != "" {
		state.upstreamSession.ChatSessionID = chatSessionID
	}
	if parent := packet.Get("parent_message_id"); parent.Exists() {
		state.upstreamSession.ParentMessageID = parent.Int()
	}
	if reserved := packet.Get("reserved_assistant_message_id"); reserved.Exists() {
		state.upstreamSession.ReservedAssistantMessageID = reserved.Int()
	}
	if userMsg := packet.Get("user_message_id"); userMsg.Exists() {
		state.upstreamSession.UserMessageID = userMsg.Int()
	}
	obj := packet.Get("obj")
	if !obj.Exists() {
		state.unknownPackets = append(state.unknownPackets, packet.Raw)
		return nil
	}
	packetType := obj.Get("type").String()
	if packetType == "" {
		state.unknownPackets = append(state.unknownPackets, obj.Raw)
		return nil
	}
	switch packetType {
	case "message_start":
		return nil
	case "message_delta":
		text := obj.Get("content").String()
		if text == "" {
			return nil
		}
		state.textBuffer.WriteString(text)
		return writer.writeEvents(state.delta(text))
	case "reasoning_start":
		return nil
	case "reasoning_delta":
		// Onyx sends hidden thinking as reasoning_delta packets. Do not expose it
		// as assistant content; only message_delta belongs in the API response.
		reasoning := obj.Get("reasoning").String()
		if reasoning != "" {
			state.reasoningBuffer.WriteString(reasoning)
		}
		return nil
	case "section_end", "stop":
		return nil
	case "python_tool_start":
		state.toolEvents = append(state.toolEvents, webOnyxToolEvent{
			Kind:  "python",
			Phase: "start",
			Input: map[string]any{
				"code": obj.Get("code").String(),
			},
		})
		return nil
	case "python_tool_delta":
		state.toolEvents = append(state.toolEvents, webOnyxToolEvent{
			Kind:  "python",
			Phase: "delta",
			Output: map[string]any{
				"stdout":   obj.Get("stdout").String(),
				"stderr":   obj.Get("stderr").String(),
				"file_ids": jsonValueOrFallback(obj.Get("file_ids"), []any{}),
			},
		})
		if stdout := obj.Get("stdout").String(); stdout != "" {
			state.artifacts = append(state.artifacts, webOnyxArtifact{
				Kind: "python_stdout",
				Data: map[string]any{"stdout": stdout},
			})
		}
		if stderr := obj.Get("stderr").String(); stderr != "" {
			state.artifacts = append(state.artifacts, webOnyxArtifact{
				Kind: "python_stderr",
				Data: map[string]any{"stderr": stderr},
			})
		}
		return nil
	case "open_url_start":
		state.toolEvents = append(state.toolEvents, webOnyxToolEvent{Kind: "open_url", Phase: "start"})
		return nil
	case "open_url_urls":
		urls := jsonValueOrFallback(obj.Get("urls"), []any{})
		state.toolEvents = append(state.toolEvents, webOnyxToolEvent{
			Kind:  "open_url",
			Phase: "input",
			Input: map[string]any{"urls": urls},
		})
		return nil
	case "open_url_documents":
		documents := jsonValueOrFallback(obj.Get("documents"), []any{})
		state.toolEvents = append(state.toolEvents, webOnyxToolEvent{
			Kind:  "open_url",
			Phase: "documents",
			Output: map[string]any{
				"documents": documents,
			},
		})
		appendOnyxDocuments(state, obj.Get("documents"))
		return nil
	case "citation_info":
		citation := webOnyxCitation{
			CitationNumber: obj.Get("citation_number").Int(),
			DocumentID:     obj.Get("document_id").String(),
		}
		state.citations = append(state.citations, citation)
		return nil
	case "image_generation_start":
		state.toolEvents = append(state.toolEvents, webOnyxToolEvent{Kind: "generate_image", Phase: "start"})
		return nil
	case "image_generation_final":
		images := jsonValueOrFallback(obj.Get("images"), []any{})
		state.toolEvents = append(state.toolEvents, webOnyxToolEvent{
			Kind:  "generate_image",
			Phase: "final",
			Output: map[string]any{
				"images": images,
			},
		})
		appendOnyxImages(state, obj.Get("images"))
		return nil
	case "error":
		msg := firstNonEmptyString(
			obj.Get("message").String(),
			obj.Get("error").String(),
			obj.Get("detail").String(),
			obj.Get("exception").String(),
		)
		if msg == "" {
			msg = "onyx upstream returned an error packet"
		}
		return fmt.Errorf("%s", sanitizeUpstreamErrorMessage(msg))
	default:
		state.unknownPackets = append(state.unknownPackets, obj.Raw)
		return nil
	}
}

func jsonValueOrFallback(res gjson.Result, fallback any) any {
	if !res.Exists() {
		return fallback
	}
	return res.Value()
}

func appendOnyxDocuments(state *webOnyxStreamState, docs gjson.Result) {
	if !docs.Exists() || !docs.IsArray() {
		return
	}
	for _, doc := range docs.Array() {
		state.finalDocuments = append(state.finalDocuments, webOnyxDocument{
			DocumentID:         doc.Get("document_id").String(),
			Link:               doc.Get("link").String(),
			SemanticIdentifier: doc.Get("semantic_identifier").String(),
			SourceType:         doc.Get("source_type").String(),
			Raw:                doc.Value(),
		})
	}
}

func appendOnyxImages(state *webOnyxStreamState, images gjson.Result) {
	if !images.Exists() || !images.IsArray() {
		return
	}
	for _, image := range images.Array() {
		state.artifacts = append(state.artifacts, webOnyxArtifact{
			Kind: "image",
			Data: map[string]any{
				"file_id":        image.Get("file_id").String(),
				"url":            image.Get("url").String(),
				"revised_prompt": image.Get("revised_prompt").String(),
				"shape":          image.Get("shape").String(),
			},
		})
	}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func scanWebSSE(r io.Reader, maxLineSize int, handle func(eventName, payload string) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineSize)
	eventName := ""
	var dataLines []string
	flush := func() error {
		if len(dataLines) == 0 {
			eventName = ""
			return nil
		}
		payload := strings.Join(dataLines, "\n")
		dataLines = nil
		name := eventName
		eventName = ""
		return handle(name, payload)
	}
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "event:"):
			eventName = strings.TrimSpace(strings.TrimPrefix(trimmed, "event:"))
		case strings.HasPrefix(trimmed, "data:"):
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(trimmed, "data:")))
		case strings.HasPrefix(trimmed, ":"):
			continue
		default:
			if err := flush(); err != nil {
				return err
			}
			if err := handle("", trimmed); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func (s *WebGatewayService) maxWebStreamLineSize() int {
	if s != nil && s.cfg != nil && s.cfg.Gateway.MaxLineSize > 0 {
		return s.cfg.Gateway.MaxLineSize
	}
	return defaultMaxLineSize
}

func webChatIncludeUsage(body []byte) bool {
	return gjson.GetBytes(body, "stream_options.include_usage").Bool()
}

type webTextToAnthropicStreamState struct {
	model     string
	started   bool
	textOpen  bool
	blockIdx  int
	messageID string
}

type webOnyxToolEvent struct {
	Kind   string
	Phase  string
	Input  map[string]any
	Output map[string]any
}

type webOnyxCitation struct {
	CitationNumber int64
	DocumentID     string
}

type webOnyxArtifact struct {
	Kind string
	Data map[string]any
}

type webOnyxDocument struct {
	DocumentID         string
	Link               string
	SemanticIdentifier string
	SourceType         string
	Raw                any
}

type webOnyxUpstreamSession struct {
	ChatSessionID              string
	ParentMessageID            int64
	ReservedAssistantMessageID int64
	UserMessageID              int64
}

type webOnyxStreamState struct {
	text            *webTextToAnthropicStreamState
	messageID       string
	textBuffer      strings.Builder
	reasoningBuffer strings.Builder
	toolEvents      []webOnyxToolEvent
	citations       []webOnyxCitation
	artifacts       []webOnyxArtifact
	finalDocuments  []webOnyxDocument
	usage           ClaudeUsage
	upstreamSession webOnyxUpstreamSession
	unknownPackets  []string
}

type webOnyxPreparedRequest struct {
	SessionKey            string
	GroupID               int64
	AccountID             int64
	SessionHash           string
	BindingSource         string
	ChatSessionID         string
	Recoverable           bool
	ExplicitChatSessionID bool
	ExplicitParentMessage bool
}

type webOnyxSessionStore struct {
	ttl                 time.Duration
	mu                  sync.RWMutex
	sessions            map[string]OnyxSessionBinding
	lastCleanupUnixNano atomic.Int64
}

const (
	webOnyxSessionStoreCleanupInterval = time.Minute
	webOnyxSessionStoreMaxEntries      = 65536
	webOnyxSessionStoreCleanupMaxScan  = 512
)

func newWebOnyxSessionStore(ttl time.Duration) *webOnyxSessionStore {
	store := &webOnyxSessionStore{
		ttl:      normalizeWebOnyxTTL(ttl),
		sessions: make(map[string]OnyxSessionBinding, 256),
	}
	store.lastCleanupUnixNano.Store(time.Now().UnixNano())
	return store
}

func (s *webOnyxSessionStore) Get(sessionKey string) (OnyxSessionBinding, bool) {
	if s == nil {
		return OnyxSessionBinding{}, false
	}
	key := strings.TrimSpace(sessionKey)
	if key == "" {
		return OnyxSessionBinding{}, false
	}
	s.maybeCleanup()
	now := time.Now()
	s.mu.RLock()
	binding, ok := s.sessions[key]
	s.mu.RUnlock()
	if !ok || (!binding.ExpiresAt.IsZero() && now.After(binding.ExpiresAt)) {
		return OnyxSessionBinding{}, false
	}
	return binding, true
}

func (s *webOnyxSessionStore) Upsert(sessionKey string, update OnyxSessionBinding) {
	if s == nil {
		return
	}
	key := strings.TrimSpace(sessionKey)
	if key == "" {
		return
	}
	s.maybeCleanup()
	now := time.Now()
	s.mu.Lock()
	current := s.sessions[key]
	merged := mergeOnyxSessionBinding(current, update, now, s.ttl)
	ensureBindingCapacity(s.sessions, key, webOnyxSessionStoreMaxEntries)
	s.sessions[key] = merged
	s.mu.Unlock()
}

func (s *webOnyxSessionStore) Delete(sessionKey string) {
	if s == nil {
		return
	}
	key := strings.TrimSpace(sessionKey)
	if key == "" {
		return
	}
	s.mu.Lock()
	delete(s.sessions, key)
	s.mu.Unlock()
}

func (s *webOnyxSessionStore) maybeCleanup() {
	if s == nil {
		return
	}
	now := time.Now()
	last := time.Unix(0, s.lastCleanupUnixNano.Load())
	if now.Sub(last) < webOnyxSessionStoreCleanupInterval {
		return
	}
	if !s.lastCleanupUnixNano.CompareAndSwap(last.UnixNano(), now.UnixNano()) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cleaned := 0
	for key, binding := range s.sessions {
		if !binding.ExpiresAt.IsZero() && now.After(binding.ExpiresAt) {
			delete(s.sessions, key)
		}
		cleaned++
		if cleaned >= webOnyxSessionStoreCleanupMaxScan {
			break
		}
	}
}

func normalizeWebOnyxTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return stickySessionTTL
	}
	return ttl
}

func mergeOnyxSessionBinding(current, update OnyxSessionBinding, now time.Time, ttl time.Duration) OnyxSessionBinding {
	merged := current
	update = normalizeOnyxSessionBinding(update)
	if update.ChatSessionID != "" {
		merged.ChatSessionID = update.ChatSessionID
	}
	if update.ParentMessageID > 0 {
		merged.ParentMessageID = update.ParentMessageID
	}
	if update.ReservedAssistantMessageID > 0 {
		merged.ReservedAssistantMessageID = update.ReservedAssistantMessageID
	}
	if update.UserMessageID > 0 {
		merged.UserMessageID = update.UserMessageID
	}
	if !update.UpdatedAt.IsZero() {
		merged.UpdatedAt = update.UpdatedAt
	} else {
		merged.UpdatedAt = now
	}
	if !update.ExpiresAt.IsZero() {
		merged.ExpiresAt = update.ExpiresAt
	} else {
		merged.ExpiresAt = now.Add(normalizeWebOnyxTTL(ttl))
	}
	return merged
}

func newWebOnyxStreamState(model string) *webOnyxStreamState {
	text := newWebTextToAnthropicStreamState(model)
	return &webOnyxStreamState{
		text:      text,
		messageID: text.messageID,
	}
}

func (s *webOnyxStreamState) delta(text string) []apicompat.AnthropicStreamEvent {
	return s.text.delta(text)
}

func (s *webOnyxStreamState) finalize() []apicompat.AnthropicStreamEvent {
	return s.text.finalize()
}

func newWebTextToAnthropicStreamState(model string) *webTextToAnthropicStreamState {
	return &webTextToAnthropicStreamState{
		model:     model,
		messageID: "msg_" + uuid.NewString(),
	}
}

func (s *webTextToAnthropicStreamState) delta(text string) []apicompat.AnthropicStreamEvent {
	var events []apicompat.AnthropicStreamEvent
	if !s.started {
		s.started = true
		events = append(events, apicompat.AnthropicStreamEvent{
			Type: "message_start",
			Message: &apicompat.AnthropicResponse{
				ID:      s.messageID,
				Type:    "message",
				Role:    "assistant",
				Model:   s.model,
				Content: []apicompat.AnthropicContentBlock{},
				Usage:   apicompat.AnthropicUsage{},
			},
		})
	}
	if !s.textOpen {
		s.textOpen = true
		events = append(events, apicompat.AnthropicStreamEvent{
			Type:  "content_block_start",
			Index: webIntPtr(0),
			ContentBlock: &apicompat.AnthropicContentBlock{
				Type: "text",
				Text: "",
			},
		})
	}
	events = append(events, apicompat.AnthropicStreamEvent{
		Type:  "content_block_delta",
		Index: webIntPtr(0),
		Delta: &apicompat.AnthropicDelta{
			Type: "text_delta",
			Text: text,
		},
	})
	return events
}

func (s *webTextToAnthropicStreamState) finalize() []apicompat.AnthropicStreamEvent {
	if !s.started {
		return nil
	}
	var events []apicompat.AnthropicStreamEvent
	if s.textOpen {
		events = append(events, apicompat.AnthropicStreamEvent{
			Type:  "content_block_stop",
			Index: webIntPtr(0),
		})
	}
	events = append(events,
		apicompat.AnthropicStreamEvent{
			Type: "message_delta",
			Delta: &apicompat.AnthropicDelta{
				StopReason: "end_turn",
			},
			Usage: &apicompat.AnthropicUsage{},
		},
		apicompat.AnthropicStreamEvent{Type: "message_stop"},
	)
	return events
}

type webChatToolStreamBlock struct {
	blockIndex int
	name       string
	opened     bool
}

type webChatToAnthropicStreamState struct {
	model      string
	started    bool
	textOpen   bool
	textIndex  int
	nextIndex  int
	messageID  string
	toolBlocks map[int]*webChatToolStreamBlock
	usage      ClaudeUsage
	finalized  bool
	stopReason string
}

func newWebChatToAnthropicStreamState(model string) *webChatToAnthropicStreamState {
	return &webChatToAnthropicStreamState{
		model:      model,
		messageID:  "msg_" + uuid.NewString(),
		toolBlocks: map[int]*webChatToolStreamBlock{},
	}
}

func (s *webChatToAnthropicStreamState) process(chunk apicompat.ChatCompletionsChunk) []apicompat.AnthropicStreamEvent {
	var events []apicompat.AnthropicStreamEvent
	if chunk.ID != "" {
		s.messageID = chunk.ID
	}
	if chunk.Model != "" {
		s.model = chunk.Model
	}
	if !s.started && len(chunk.Choices) > 0 {
		s.started = true
		events = append(events, apicompat.AnthropicStreamEvent{
			Type: "message_start",
			Message: &apicompat.AnthropicResponse{
				ID:      s.messageID,
				Type:    "message",
				Role:    "assistant",
				Model:   s.model,
				Content: []apicompat.AnthropicContentBlock{},
				Usage:   apicompat.AnthropicUsage{},
			},
		})
	}
	if chunk.Usage != nil {
		s.usage.InputTokens = chunk.Usage.PromptTokens
		s.usage.OutputTokens = chunk.Usage.CompletionTokens
		if chunk.Usage.PromptTokensDetails != nil {
			s.usage.CacheReadInputTokens = chunk.Usage.PromptTokensDetails.CachedTokens
		}
	}
	for _, choice := range chunk.Choices {
		if choice.Delta.Content != nil && *choice.Delta.Content != "" {
			if !s.textOpen {
				s.textOpen = true
				idx := s.nextIndex
				s.textIndex = idx
				s.nextIndex++
				events = append(events, apicompat.AnthropicStreamEvent{
					Type:  "content_block_start",
					Index: webIntPtr(idx),
					ContentBlock: &apicompat.AnthropicContentBlock{
						Type: "text",
						Text: "",
					},
				})
			}
			events = append(events, apicompat.AnthropicStreamEvent{
				Type:  "content_block_delta",
				Index: webIntPtr(s.textIndex),
				Delta: &apicompat.AnthropicDelta{
					Type: "text_delta",
					Text: *choice.Delta.Content,
				},
			})
		}
		if len(choice.Delta.ToolCalls) > 0 {
			if s.textOpen {
				events = append(events, apicompat.AnthropicStreamEvent{
					Type:  "content_block_stop",
					Index: webIntPtr(s.textIndex),
				})
				s.textOpen = false
			}
			for _, tc := range choice.Delta.ToolCalls {
				idx := 0
				if tc.Index != nil {
					idx = *tc.Index
				}
				block := s.toolBlocks[idx]
				if block == nil {
					block = &webChatToolStreamBlock{blockIndex: s.nextIndex}
					s.nextIndex++
					s.toolBlocks[idx] = block
				}
				if tc.Function.Name != "" {
					block.name = tc.Function.Name
				}
				if !block.opened {
					block.opened = true
					name := block.name
					if name == "" {
						name = "tool"
					}
					id := tc.ID
					if id == "" {
						id = "toolu_" + uuid.NewString()
					}
					events = append(events, apicompat.AnthropicStreamEvent{
						Type:  "content_block_start",
						Index: webIntPtr(block.blockIndex),
						ContentBlock: &apicompat.AnthropicContentBlock{
							Type:  "tool_use",
							ID:    id,
							Name:  name,
							Input: json.RawMessage(`{}`),
						},
					})
				}
				if tc.Function.Arguments != "" {
					events = append(events, apicompat.AnthropicStreamEvent{
						Type:  "content_block_delta",
						Index: webIntPtr(block.blockIndex),
						Delta: &apicompat.AnthropicDelta{
							Type:        "input_json_delta",
							PartialJSON: tc.Function.Arguments,
						},
					})
				}
			}
		}
		if choice.FinishReason != nil {
			s.stopReason = normalizeWebStopReason(*choice.FinishReason, len(s.toolBlocks) > 0)
			events = append(events, s.finalize()...)
		}
	}
	return events
}

func (s *webChatToAnthropicStreamState) finalize() []apicompat.AnthropicStreamEvent {
	if !s.started || s.finalized {
		return nil
	}
	s.finalized = true
	var events []apicompat.AnthropicStreamEvent
	if s.textOpen {
		events = append(events, apicompat.AnthropicStreamEvent{
			Type:  "content_block_stop",
			Index: webIntPtr(s.textIndex),
		})
	}
	for _, block := range s.toolBlocks {
		if block.opened {
			events = append(events, apicompat.AnthropicStreamEvent{
				Type:  "content_block_stop",
				Index: webIntPtr(block.blockIndex),
			})
		}
	}
	stopReason := s.stopReason
	if stopReason == "" {
		stopReason = normalizeWebStopReason("", len(s.toolBlocks) > 0)
	}
	events = append(events,
		apicompat.AnthropicStreamEvent{
			Type: "message_delta",
			Delta: &apicompat.AnthropicDelta{
				StopReason: stopReason,
			},
			Usage: ptrAnthropicUsage(anthropicUsageFromClaude(s.usage)),
		},
		apicompat.AnthropicStreamEvent{Type: "message_stop"},
	)
	return events
}

func webIntPtr(v int) *int {
	return &v
}

func writeWebError(c *gin.Context, format webForwardFormat, status int, code, message string) {
	switch format {
	case webForwardChatCompletions:
		c.JSON(status, gin.H{"error": gin.H{"type": code, "message": message}})
	case webForwardResponses:
		c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
	default:
		c.JSON(status, gin.H{"type": "error", "error": gin.H{"type": code, "message": message}})
	}
}

func webOpenAIUsage(usage ClaudeUsage) gin.H {
	input := usage.InputTokens + usage.CacheReadInputTokens + usage.CacheCreationInputTokens
	output := usage.OutputTokens
	return gin.H{
		"input_tokens":  input,
		"output_tokens": output,
		"total_tokens":  input + output,
	}
}

func webChatCompletionsUsage(usage ClaudeUsage) gin.H {
	input := usage.InputTokens + usage.CacheReadInputTokens + usage.CacheCreationInputTokens
	output := usage.OutputTokens
	return gin.H{
		"prompt_tokens":     input,
		"completion_tokens": output,
		"total_tokens":      input + output,
	}
}

func optionalUpstreamModel(requested, upstream string) string {
	if upstream == "" || upstream == requested {
		return ""
	}
	return upstream
}

func webModelAndStream(body []byte, parsed *ParsedRequest) (string, bool) {
	bodyModel := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	bodyStream := gjson.GetBytes(body, "stream").Bool()
	if bodyModel != "" {
		return bodyModel, bodyStream
	}
	if parsed != nil && parsed.Model != "" {
		return parsed.Model, parsed.Stream
	}
	return bodyModel, bodyStream
}

func webExtractMessages(body []byte) []any {
	msgs := gjson.GetBytes(body, "messages")
	if !msgs.Exists() || !msgs.IsArray() {
		return nil
	}
	var out []any
	_ = json.Unmarshal([]byte(msgs.Raw), &out)
	return out
}

func webExtractResponsesInput(body []byte) []any {
	input := gjson.GetBytes(body, "input")
	if !input.Exists() {
		return nil
	}
	if input.Type == gjson.String {
		return []any{map[string]any{"role": "user", "content": input.String()}}
	}
	if input.IsArray() {
		var out []any
		_ = json.Unmarshal([]byte(input.Raw), &out)
		return out
	}
	return nil
}

func webMessagesJSON(messages []any) string {
	if len(messages) == 0 {
		return "[]"
	}
	b, err := json.Marshal(messages)
	if err != nil {
		return "[]"
	}
	return string(b)
}

func webBuildPrompt(system any, messages []any, body []byte) string {
	var parts []string
	if sys := webTextFromAny(system); sys != "" {
		parts = append(parts, "system: "+sys)
	}
	for _, msg := range messages {
		if m, ok := msg.(map[string]any); ok {
			role, _ := m["role"].(string)
			text := webTextFromAny(m["content"])
			if text == "" {
				continue
			}
			if role == "" {
				parts = append(parts, text)
			} else {
				parts = append(parts, role+": "+text)
			}
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, "\n")
	}
	if s := strings.TrimSpace(gjson.GetBytes(body, "prompt").String()); s != "" {
		return s
	}
	if s := strings.TrimSpace(gjson.GetBytes(body, "input").String()); s != "" {
		return s
	}
	return strings.TrimSpace(string(body))
}

func webTextFromAny(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if text := webTextFromAny(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		if text, ok := v["text"].(string); ok {
			return text
		}
		if text, ok := v["content"].(string); ok {
			return text
		}
		if content, ok := v["content"]; ok {
			return webTextFromAny(content)
		}
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
	case json.Number:
		return v.String()
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	}
	return ""
}

func anyMap(value any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	if m, ok := value.(map[string]any); ok {
		return m
	}
	if m, ok := value.(map[string]string); ok {
		out := make(map[string]any, len(m))
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	return map[string]any{}
}

func anyStringMap(value any) map[string]string {
	if value == nil {
		return nil
	}
	if m, ok := value.(map[string]string); ok {
		return m
	}
	if m, ok := value.(map[string]any); ok {
		out := make(map[string]string, len(m))
		for k, v := range m {
			if s, ok := v.(string); ok {
				out[k] = s
			}
		}
		return out
	}
	return nil
}

func mergeStringMaps(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			if strings.TrimSpace(k) != "" {
				out[k] = v
			}
		}
	}
	return out
}

func firstString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func boolMapValue(m map[string]any, key string) bool {
	if m == nil {
		return false
	}
	switch v := m[key].(type) {
	case bool:
		return v
	case string:
		parsed, _ := strconv.ParseBool(strings.TrimSpace(v))
		return parsed
	default:
		return false
	}
}

func normalizeWebUpstreamFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "anthropic_messages", "openai_chat", "openai_responses":
		return strings.ToLower(strings.TrimSpace(format))
	default:
		return "custom_json"
	}
}

func normalizeWebStreamFormat(format, upstreamFormat string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "anthropic_sse", "openai_chat_sse", "openai_responses_sse", "custom_text_sse", "onyx_chat_sse":
		return strings.ToLower(strings.TrimSpace(format))
	}
	switch normalizeWebUpstreamFormat(upstreamFormat) {
	case "anthropic_messages":
		return "anthropic_sse"
	case "openai_chat":
		return "openai_chat_sse"
	case "openai_responses":
		return "openai_responses_sse"
	default:
		return "custom_text_sse"
	}
}
