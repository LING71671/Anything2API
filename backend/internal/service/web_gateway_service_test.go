//go:build unit

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type webGatewayTestUpstream struct {
	status    int
	header    http.Header
	body      string
	responses []webGatewayTestResponse
	calls     int
	lastReq   *http.Request
	lastBody  []byte
	requests  []*http.Request
	bodies    [][]byte
}

type webGatewayTestResponse struct {
	status int
	header http.Header
	body   string
}

type fakeOnyxSessionStore struct {
	mu       sync.Mutex
	bindings map[string]OnyxSessionBinding
	getErr   error
	setErr   error
	delErr   error
	getCalls int
	setCalls int
	delCalls int
}

func newFakeOnyxSessionStore() *fakeOnyxSessionStore {
	return &fakeOnyxSessionStore{bindings: map[string]OnyxSessionBinding{}}
}

func resetOnyxSessionMetricsTestState() {
	resetOnyxSessionMetricsForTest()
}

func (s *fakeOnyxSessionStore) Get(_ context.Context, groupID, accountID int64, sessionHash string) (OnyxSessionBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls++
	if s.getErr != nil {
		return OnyxSessionBinding{}, s.getErr
	}
	binding, ok := s.bindings[s.key(groupID, accountID, sessionHash)]
	if !ok {
		return OnyxSessionBinding{}, redis.Nil
	}
	return binding, nil
}

func (s *fakeOnyxSessionStore) Set(_ context.Context, groupID, accountID int64, sessionHash string, binding OnyxSessionBinding, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setCalls++
	if s.setErr != nil {
		return s.setErr
	}
	s.bindings[s.key(groupID, accountID, sessionHash)] = binding
	return nil
}

func (s *fakeOnyxSessionStore) Delete(_ context.Context, groupID, accountID int64, sessionHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delCalls++
	if s.delErr != nil {
		return s.delErr
	}
	delete(s.bindings, s.key(groupID, accountID, sessionHash))
	return nil
}

func (s *fakeOnyxSessionStore) key(groupID, accountID int64, sessionHash string) string {
	return strconv.FormatInt(groupID, 10) + ":" + strconv.FormatInt(accountID, 10) + ":" + sessionHash
}

func (u *webGatewayTestUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls++
	u.lastReq = req
	u.requests = append(u.requests, req.Clone(req.Context()))
	if req.Body != nil {
		u.lastBody, _ = io.ReadAll(req.Body)
		u.bodies = append(u.bodies, append([]byte(nil), u.lastBody...))
	}
	respCfg := webGatewayTestResponse{
		status: u.status,
		header: u.header,
		body:   u.body,
	}
	if idx := u.calls - 1; idx >= 0 && idx < len(u.responses) {
		respCfg = u.responses[idx]
	}
	status := respCfg.status
	if status == 0 {
		status = http.StatusOK
	}
	header := respCfg.header
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(respCfg.body)),
	}, nil
}

func (u *webGatewayTestUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxyURL, accountID, accountConcurrency)
}

func TestBuildWebRequestBody(t *testing.T) {
	body, err := buildWebRequestBody(
		`{"model":"{{model}}","provider":"{{model_provider}}","version":"{{model_version}}","prompt":"{{prompt}}","messages":{{messages_json}},"system":"{{system}}","user":"{{metadata_user_id}}"}`,
		webTemplateValues{
			Model:          `m"1`,
			ModelProvider:  "openai",
			ModelVersion:   `gpt"5.4`,
			Prompt:         "hello\nworld",
			MessagesJSON:   `[{"role":"user","content":"hi"}]`,
			System:         "be terse",
			MetadataUserID: "user-1",
		},
	)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, `m"1`, got["model"])
	require.Equal(t, "openai", got["provider"])
	require.Equal(t, `gpt"5.4`, got["version"])
	require.Equal(t, "hello\nworld", got["prompt"])
	require.Equal(t, "be terse", got["system"])
	require.Equal(t, "user-1", got["user"])
	require.Len(t, got["messages"], 1)
}

func TestSplitWebModelProviderVersion(t *testing.T) {
	provider, version := splitWebModelProviderVersion("anthropic/claude-sonnet-4.6")
	require.Equal(t, "anthropic", provider)
	require.Equal(t, "claude-sonnet-4.6", version)

	provider, version = splitWebModelProviderVersion("openai:gpt5.4")
	require.Equal(t, "openai", provider)
	require.Equal(t, "gpt5.4", version)

	provider, version = splitWebModelProviderVersion("claude-opus-4.7")
	require.Equal(t, "anthropic", provider)
	require.Equal(t, "claude-opus-4.7", version)
}

func TestMapWebRequestedModelUsesResolvedPlatformMapping(t *testing.T) {
	got := mapWebRequestedModel("gpt5.4", &Account{}, webCredentials{
		ModelMapping: DefaultOnyxWebModelMapping(),
	})
	require.Equal(t, "openai/gpt5.4", got)

	got = mapWebRequestedModel("generate_image", &Account{}, webCredentials{
		ModelMapping: DefaultOnyxWebModelMapping(),
	})
	require.Equal(t, "openai/gpt5.4", got)
}

func TestBuildWebRequestBodyProgrammingToolVariables(t *testing.T) {
	body, err := buildWebRequestBody(
		`{"model":"{{model}}","stream":{{stream}},"max_tokens":{{max_tokens}},"temperature":{{temperature}},"top_p":{{top_p}},"stop":{{stop_json}},"tools":{{tools_json}},"tool_choice":{{tool_choice_json}},"metadata":{{metadata_json}},"raw":{{raw_request_json}},"anthropic_messages":{{anthropic_messages_json}},"chat_messages":{{chat_messages_json}},"responses_input":{{responses_input_json}}}`,
		webTemplateValues{
			Model:                 "coder",
			StreamJSON:            "true",
			MaxTokensJSON:         "123",
			TemperatureJSON:       "0.2",
			TopPJSON:              "0.9",
			StopJSON:              `["END"]`,
			ToolsJSON:             `[{"type":"function","function":{"name":"read_file"}}]`,
			ToolChoiceJSON:        `{"type":"function","function":{"name":"read_file"}}`,
			MetadataJSON:          `{"user_id":"u1"}`,
			RawRequestJSON:        `{"stream":true}`,
			AnthropicMessagesJSON: `[{"role":"user","content":"hi"}]`,
			ChatMessagesJSON:      `[{"role":"tool","tool_call_id":"call_1","content":"ok"}]`,
			ResponsesInputJSON:    `[{"type":"function_call_output","call_id":"call_1","output":"ok"}]`,
		},
	)
	require.NoError(t, err)
	require.True(t, gjsonValidObjectPath(body, "stream"))
	require.Equal(t, "read_file", jsonPathString(body, "tools.0.function.name"))
	require.Equal(t, "call_1", jsonPathString(body, "chat_messages.0.tool_call_id"))
	require.Equal(t, "function_call_output", jsonPathString(body, "responses_input.0.type"))
}

func TestBuildWebRequestBodyRejectsInvalidJSON(t *testing.T) {
	_, err := buildWebRequestBody(`{"prompt":{{prompt}}}`, webTemplateValues{Prompt: "hello"})
	require.ErrorContains(t, err, "invalid JSON")
}

func TestParseWebCredentials(t *testing.T) {
	account := &Account{
		Credentials: map[string]any{
			"base_url":         "https://example.com/generate",
			"response":         map[string]any{"text_path": "data.text"},
			"request_template": `{"prompt":"{{prompt}}"}`,
		},
	}

	creds, err := parseWebCredentials(account)
	require.NoError(t, err)
	require.Equal(t, http.MethodPost, creds.Method)
	require.Equal(t, "none", creds.Auth.Type)
	require.Equal(t, "data.text", creds.Response.TextPath)
}

func TestParseWebCredentialsRequiresTextPath(t *testing.T) {
	_, err := parseWebCredentials(&Account{Credentials: map[string]any{"base_url": "https://example.com"}})
	require.ErrorContains(t, err, "response.text_path or response.tool_calls.path")
}

func TestParseWebCredentialsAllowsToolOnlyCustomJSON(t *testing.T) {
	creds, err := parseWebCredentials(&Account{Credentials: map[string]any{
		"base_url": "https://example.com",
		"response": map[string]any{
			"tool_calls": map[string]any{"path": "choices.0.message.tool_calls"},
		},
	}})
	require.NoError(t, err)
	require.Equal(t, "choices.0.message.tool_calls", creds.Response.ToolCalls.Path)
}

func TestParseWebResponse(t *testing.T) {
	text, requestID, model, usage, err := parseWebResponse(
		[]byte(`{"id":"req_1","model":"remote-model","data":{"text":"ok"},"usage":{"input":7,"output":3}}`),
		webResponseConfig{
			TextPath:      "data.text",
			RequestIDPath: "id",
			ModelPath:     "model",
			UsagePaths: webUsagePaths{
				InputTokens:  "usage.input",
				OutputTokens: "usage.output",
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, "ok", text)
	require.Equal(t, "req_1", requestID)
	require.Equal(t, "remote-model", model)
	require.Equal(t, 7, usage.InputTokens)
	require.Equal(t, 3, usage.OutputTokens)
}

func TestParseWebResponseMissingTextPath(t *testing.T) {
	_, _, _, _, err := parseWebResponse([]byte(`{"data":{"message":"ok"}}`), webResponseConfig{TextPath: "data.text"})
	require.ErrorContains(t, err, "missing text_path")
}

func TestWebGatewayNonStreamEstimatesUsageWhenUpstreamMissingUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &webGatewayTestUpstream{body: `{"id":"msg_1","answer":"Hello from Onyx"}`}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestAccount(map[string]any{
		"base_url":         "https://example.com/chat",
		"request_template": `{"message":"{{prompt}}","model":"{{model}}"}`,
		"response": map[string]any{
			"text_path":       "answer",
			"request_id_path": "id",
		},
	})
	body := []byte(`{"model":"gpt5.4","messages":[{"role":"user","content":"say hello for a coding test"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, nil)
	require.NoError(t, err)
	require.False(t, result.Stream)
	require.True(t, result.UsageEstimated)
	require.Greater(t, result.Usage.InputTokens, 0)
	require.Greater(t, result.Usage.OutputTokens, 0)
	require.Equal(t, "msg_1", result.RequestID)
	require.Equal(t, "Hello from Onyx", jsonPathString(rec.Body.Bytes(), "choices.0.message.content"))
}

func TestWithWebSessionDiagnostic(t *testing.T) {
	msg := withWebSessionDiagnostic(http.StatusForbidden, http.Header{}, "forbidden")
	require.Contains(t, msg, "web session rejected")
	require.Contains(t, msg, "CSRF/header")

	msg = withWebSessionDiagnostic(http.StatusTooManyRequests, http.Header{"Retry-After": []string{"60"}}, "slow down")
	require.Contains(t, msg, "retry-after=60")
}

func TestWebGatewayNonStreamConfiguredToolCallsReachChatCompletions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &webGatewayTestUpstream{body: `{
		"id":"req_1",
		"model":"remote-coder",
		"choices":[{"message":{"tool_calls":[{"id":"call_1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"main.go\"}"}}]},"finish_reason":"tool_calls"}],
		"usage":{"prompt_tokens":11,"completion_tokens":7}
	}`}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestAccount(map[string]any{
		"base_url":         "https://example.com/chat",
		"request_template": `{"model":"{{model}}","messages":{{chat_messages_json}},"tools":{{tools_json}}}`,
		"upstream_format":  "custom_json",
		"response":         webGatewayOpenAIChatResponseConfig(),
		"model_mapping":    map[string]any{"coder": "remote-coder"},
	})
	body := []byte(`{"model":"coder","messages":[{"role":"user","content":"read main.go"}],"tools":[{"type":"function","function":{"name":"read_file","parameters":{"type":"object"}}}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, nil)
	require.NoError(t, err)
	require.False(t, result.Stream)
	require.Equal(t, "req_1", result.RequestID)
	require.Equal(t, 11, result.Usage.InputTokens)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "tool_calls", jsonPathString(rec.Body.Bytes(), "choices.0.finish_reason"))
	require.Equal(t, "read_file", jsonPathString(rec.Body.Bytes(), "choices.0.message.tool_calls.0.function.name"))
	require.Equal(t, `{"path":"main.go"}`, jsonPathString(rec.Body.Bytes(), "choices.0.message.tool_calls.0.function.arguments"))
	require.Equal(t, "remote-coder", jsonPathString(upstream.lastBody, "model"))
}

func TestWebGatewayStreamsOpenAIChatToolCallsToChatCompletions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &webGatewayTestUpstream{
		header: http.Header{"Content-Type": []string{"text/event-stream"}},
		body: strings.Join([]string{
			`data: {"id":"chatcmpl_1","object":"chat.completion.chunk","created":1,"model":"remote-coder","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`,
			``,
			`data: {"id":"chatcmpl_1","object":"chat.completion.chunk","created":1,"model":"remote-coder","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read_file","arguments":""}}]},"finish_reason":null}]}`,
			``,
			`data: {"id":"chatcmpl_1","object":"chat.completion.chunk","created":1,"model":"remote-coder","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"path\""}}]},"finish_reason":null}]}`,
			``,
			`data: {"id":"chatcmpl_1","object":"chat.completion.chunk","created":1,"model":"remote-coder","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":":\"main.go\"}"}}]},"finish_reason":null}]}`,
			``,
			`data: {"id":"chatcmpl_1","object":"chat.completion.chunk","created":1,"model":"remote-coder","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}`,
			``,
			`data: [DONE]`,
			``,
		}, "\n"),
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestAccount(map[string]any{
		"base_url":                "https://example.com/chat",
		"request_template":        `{"model":"{{model}}","messages":{{chat_messages_json}},"tools":{{tools_json}},"stream":{{stream}}}`,
		"stream_request_template": `{"model":"{{model}}","messages":{{chat_messages_json}},"tools":{{tools_json}},"stream":true}`,
		"supports_stream":         true,
		"upstream_format":         "openai_chat",
		"response": map[string]any{
			"stream": map[string]any{"format": "openai_chat_sse"},
		},
	})
	body := []byte(`{"model":"coder","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"read main.go"}],"tools":[{"type":"function","function":{"name":"read_file","parameters":{"type":"object"}}}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, nil)
	require.NoError(t, err)
	require.True(t, result.Stream)
	require.Equal(t, 11, result.Usage.InputTokens)
	out := rec.Body.String()
	require.Contains(t, out, `"tool_calls"`)
	require.Contains(t, out, `"name":"read_file"`)
	require.Contains(t, out, `\"main.go\"`)
	require.Contains(t, out, `"finish_reason":"tool_calls"`)
	require.Equal(t, 1, upstream.calls)
	require.True(t, jsonPathBool(upstream.lastBody, "stream"))
}

func TestWebGatewayRejectsUnsupportedStreamWithoutUpstreamCall(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &webGatewayTestUpstream{body: `{}`}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestAccount(map[string]any{
		"base_url":         "https://example.com/chat",
		"request_template": `{"model":"{{model}}","stream":{{stream}}}`,
		"response":         map[string]any{"text_path": "text"},
	})
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	_, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, nil)
	require.ErrorContains(t, err, "stream is not supported")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, 0, upstream.calls)
}

func TestWebGatewayStreamFormatsReachChatCompletions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name           string
		upstreamFormat string
		streamFormat   string
		responseConfig map[string]any
		body           string
		want           string
		notWant        string
	}{
		{
			name:           "anthropic_sse",
			upstreamFormat: "anthropic_messages",
			streamFormat:   "anthropic_sse",
			body: strings.Join([]string{
				`event: message_start`,
				`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"remote","content":[],"usage":{"input_tokens":2,"output_tokens":0}}}`,
				``,
				`event: content_block_start`,
				`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
				``,
				`event: content_block_delta`,
				`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"anthropic ok"}}`,
				``,
				`event: content_block_stop`,
				`data: {"type":"content_block_stop","index":0}`,
				``,
				`event: message_delta`,
				`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}`,
				``,
				`event: message_stop`,
				`data: {"type":"message_stop"}`,
				``,
			}, "\n"),
			want: "anthropic ok",
		},
		{
			name:           "openai_responses_sse",
			upstreamFormat: "openai_responses",
			streamFormat:   "openai_responses_sse",
			body: strings.Join([]string{
				`data: {"type":"response.created","response":{"id":"resp_1","object":"response","model":"remote","status":"in_progress","output":[]}}`,
				``,
				`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"responses ok"}`,
				``,
				`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"remote","status":"completed","output":[],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}`,
				``,
			}, "\n"),
			want: "responses ok",
		},
		{
			name:           "custom_text_sse",
			upstreamFormat: "custom_json",
			streamFormat:   "custom_text_sse",
			responseConfig: map[string]any{"text_path": "delta", "stream": map[string]any{"format": "custom_text_sse", "text_path": "delta"}},
			body: strings.Join([]string{
				`data: {"delta":"custom ok"}`,
				``,
				`data: [DONE]`,
				``,
			}, "\n"),
			want: "custom ok",
		},
		{
			name:           "onyx_chat_sse",
			upstreamFormat: "custom_json",
			streamFormat:   "onyx_chat_sse",
			responseConfig: map[string]any{"text_path": "answer", "stream": map[string]any{"format": "onyx_chat_sse"}},
			body: strings.Join([]string{
				`data: {"placement":{"answer_type":"regular"},"obj":{"type":"message_start"}}`,
				``,
				`data: {"placement":{"answer_type":"regular"},"obj":{"type":"message_delta","content":"onyx ok"}}`,
				``,
				`data: {"placement":{"answer_type":"regular"},"obj":{"type":"stop","stop_reason":"complete"}}`,
				``,
			}, "\n"),
			want: "onyx ok",
		},
		{
			name:           "onyx_chat_sse_skips_reasoning_delta",
			upstreamFormat: "custom_json",
			streamFormat:   "onyx_chat_sse",
			responseConfig: map[string]any{"text_path": "answer", "stream": map[string]any{"format": "onyx_chat_sse"}},
			body: strings.Join([]string{
				`data: {"placement":{"answer_type":"regular"},"obj":{"type":"message_start"}}`,
				``,
				`data: {"placement":{"answer_type":"regular"},"obj":{"type":"reasoning_delta","reasoning":"hidden thought"}}`,
				``,
				`data: {"placement":{"answer_type":"regular"},"obj":{"type":"message_delta","content":"visible answer"}}`,
				``,
				`data: {"placement":{"answer_type":"regular"},"obj":{"type":"stop","stop_reason":"complete"}}`,
				``,
			}, "\n"),
			want:    "visible answer",
			notWant: "hidden thought",
		},
		{
			name:           "onyx_chat_sse_records_python_packets_without_leaking_output",
			upstreamFormat: "custom_json",
			streamFormat:   "onyx_chat_sse",
			responseConfig: map[string]any{"text_path": "answer", "stream": map[string]any{"format": "onyx_chat_sse"}},
			body: strings.Join([]string{
				`data: {"obj":{"type":"python_tool_start","code":"print('hello')"}}`,
				``,
				`data: {"obj":{"type":"python_tool_delta","stdout":"tool stdout","stderr":"","file_ids":["file_1"]}}`,
				``,
				`data: {"obj":{"type":"message_delta","content":"visible answer"}}`,
				``,
				`data: {"obj":{"type":"stop","stop_reason":"complete"}}`,
				``,
			}, "\n"),
			want:    "visible answer",
			notWant: "tool stdout",
		},
		{
			name:           "onyx_chat_sse_records_open_url_packets_without_leaking_documents",
			upstreamFormat: "custom_json",
			streamFormat:   "onyx_chat_sse",
			responseConfig: map[string]any{"text_path": "answer", "stream": map[string]any{"format": "onyx_chat_sse"}},
			body: strings.Join([]string{
				`data: {"obj":{"type":"open_url_start"}}`,
				``,
				`data: {"obj":{"type":"open_url_urls","urls":["https://example.com"]}}`,
				``,
				`data: {"obj":{"type":"open_url_documents","documents":[{"document_id":"WEB_SEARCH_DOC_https://example.com","link":"https://example.com","semantic_identifier":"Example Domain","source_type":"web","match_highlights":["full html should stay internal"]}]}}`,
				``,
				`data: {"obj":{"type":"citation_info","citation_number":1,"document_id":"WEB_SEARCH_DOC_https://example.com"}}`,
				``,
				`data: {"obj":{"type":"message_delta","content":"example summary"}}`,
				``,
				`data: {"obj":{"type":"stop"}}`,
				``,
			}, "\n"),
			want:    "example summary",
			notWant: "full html should stay internal",
		},
		{
			name:           "onyx_chat_sse_records_image_packets_without_leaking_artifact_json",
			upstreamFormat: "custom_json",
			streamFormat:   "onyx_chat_sse",
			responseConfig: map[string]any{"text_path": "answer", "stream": map[string]any{"format": "onyx_chat_sse"}},
			body: strings.Join([]string{
				`data: {"obj":{"type":"image_generation_start"}}`,
				``,
				`data: {"obj":{"type":"image_generation_final","images":[{"file_id":"file_img_1","url":"/api/chat/file/file_img_1","revised_prompt":"red apple","shape":"square"}]}}`,
				``,
				`data: {"obj":{"type":"message_delta","content":"red apple image"}}`,
				``,
				`data: {"obj":{"type":"stop"}}`,
				``,
			}, "\n"),
			want:    "red apple image",
			notWant: "file_img_1",
		},
		{
			name:           "onyx_chat_sse_ignores_unknown_packets",
			upstreamFormat: "custom_json",
			streamFormat:   "onyx_chat_sse",
			responseConfig: map[string]any{"text_path": "answer", "stream": map[string]any{"format": "onyx_chat_sse"}},
			body: strings.Join([]string{
				`data: {"obj":{"type":"mystery_packet","payload":"secret"}}`,
				``,
				`data: {"obj":{"type":"message_delta","content":"safe answer"}}`,
				``,
				`data: {"obj":{"type":"stop"}}`,
				``,
			}, "\n"),
			want:    "safe answer",
			notWant: "secret",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &webGatewayTestUpstream{header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: tt.body}
			svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
			respCfg := tt.responseConfig
			if respCfg == nil {
				respCfg = map[string]any{"stream": map[string]any{"format": tt.streamFormat}}
			}
			account := webGatewayTestAccount(map[string]any{
				"base_url":         "https://example.com/chat",
				"request_template": `{"model":"{{model}}","messages":{{chat_messages_json}},"stream":true}`,
				"supports_stream":  true,
				"upstream_format":  tt.upstreamFormat,
				"response":         respCfg,
			})
			body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

			result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, nil)
			require.NoError(t, err)
			require.True(t, result.Stream)
			require.Contains(t, rec.Body.String(), tt.want)
			if tt.notWant != "" {
				require.NotContains(t, rec.Body.String(), tt.notWant)
			}
		})
	}
}

func TestHandleWebOnyxPacketCapturesStructuredState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	writer := newWebAnthropicStreamWriter(c, webForwardAnthropic, "onyx-model", "req_1", false)
	state := newWebOnyxStreamState("onyx-model")

	payload := `[
		{"reserved_assistant_message_id":22},
		{"obj":{"type":"reasoning_delta","reasoning":"hidden thought"}},
		{"obj":{"type":"python_tool_start","code":"print('hello')"}},
		{"obj":{"type":"python_tool_delta","stdout":"hello\n","stderr":"","file_ids":["file_python_1"]}},
		{"obj":{"type":"open_url_start"}},
		{"obj":{"type":"open_url_urls","urls":["https://example.com"]}},
		{"obj":{"type":"open_url_documents","documents":[{"document_id":"doc_1","link":"https://example.com","semantic_identifier":"Example Domain","source_type":"web"}]}},
		{"obj":{"type":"citation_info","citation_number":1,"document_id":"doc_1"}},
		{"obj":{"type":"image_generation_start"}},
		{"obj":{"type":"image_generation_final","images":[{"file_id":"img_1","url":"/api/chat/file/img_1","revised_prompt":"red apple","shape":"square"}]}},
		{"obj":{"type":"message_delta","content":"visible answer"}}
	]`

	require.NoError(t, handleWebOnyxPacketPayload(payload, state, writer))
	require.NoError(t, writer.writeEvents(state.finalize()))

	require.Equal(t, int64(22), state.upstreamSession.ReservedAssistantMessageID)
	require.Equal(t, "hidden thought", state.reasoningBuffer.String())
	require.Equal(t, "visible answer", state.textBuffer.String())
	require.Len(t, state.toolEvents, 7)
	require.Len(t, state.finalDocuments, 1)
	require.Len(t, state.citations, 1)
	require.Len(t, state.artifacts, 2)
	require.Equal(t, "python", state.toolEvents[0].Kind)
	require.Equal(t, "open_url", state.toolEvents[2].Kind)
	require.Equal(t, "generate_image", state.toolEvents[5].Kind)
	out := rec.Body.String()
	require.Contains(t, out, "visible answer")
	require.NotContains(t, out, "hidden thought")
	require.NotContains(t, out, "hello\\n")
	require.NotContains(t, out, "img_1")
}

func TestWebGatewayOnyxSessionAwareStreamingCreatesAndPersistsSession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(7)
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"session_1"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":101,\"user_message_id\":100}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      true,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_1",
		GroupID:     &groupID,
	}
	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)
	require.True(t, result.Stream)
	require.Len(t, upstream.requests, 2)
	require.Equal(t, http.MethodPut, upstream.requests[0].Method)
	require.Contains(t, upstream.requests[0].URL.String(), "/chat/create-chat-session")
	require.Contains(t, string(upstream.bodies[1]), `"chat_session_id":"session_1"`)
	require.NotContains(t, string(upstream.bodies[1]), `"parent_message_id"`)

	binding, ok := svc.onyxSessions.Get("7:1:sess_1")
	require.True(t, ok)
	require.Equal(t, "session_1", binding.ChatSessionID)
	require.Equal(t, int64(101), binding.ReservedAssistantMessageID)
	require.Equal(t, int64(101), binding.ParentMessageID)
}

func TestWebGatewayOnyxSessionAwareStreamingFollowupUsesStoredParent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(7)
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"session_1"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":101,\"user_message_id\":100}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello\"}}\n\ndata: [DONE]\n\n"},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":102,\"user_message_id\":101}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"again\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)

	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
		parsed := &ParsedRequest{
			Body:        body,
			Model:       "coder",
			Stream:      true,
			Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
			SessionHash: "sess_follow",
			GroupID:     &groupID,
		}
		_, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
		require.NoError(t, err)
	}

	require.Len(t, upstream.requests, 3)
	require.Contains(t, string(upstream.bodies[2]), `"chat_session_id":"session_1"`)
	require.Contains(t, string(upstream.bodies[2]), `"parent_message_id":101`)

	binding, ok := svc.onyxSessions.Get("7:1:sess_follow")
	require.True(t, ok)
	require.Equal(t, int64(102), binding.ReservedAssistantMessageID)
	require.Equal(t, int64(102), binding.ParentMessageID)
}

func TestWebGatewayOnyxSessionAwareNonStreamingRefreshesLatestAssistantMessage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(9)
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"session_nonstream"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"answer":"done","message_id":55}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"session_nonstream","messages":[{"message_id":54,"message_type":"assistant","error":"failed"},{"message_id":58,"message_type":"assistant","error":null},{"message_id":59,"message_type":"assistant","error":null}]}`},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":false,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      false,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_nonstream",
		GroupID:     &groupID,
	}
	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)
	require.False(t, result.Stream)
	require.Len(t, upstream.requests, 3)
	require.Contains(t, upstream.requests[2].URL.String(), "/chat/get-chat-session/session_nonstream")

	binding, ok := svc.onyxSessions.Get("9:1:sess_nonstream")
	require.True(t, ok)
	require.Equal(t, "session_nonstream", binding.ChatSessionID)
	require.Equal(t, int64(59), binding.ParentMessageID)
}

func TestWebGatewayOnyxSessionAwareInvalidSessionRetriesWithFreshSession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(11)
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"old_session"}`},
			{status: http.StatusBadRequest, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"message":"invalid session id"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"new_session"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":501,\"user_message_id\":500}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"recovered\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      true,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_retry",
		GroupID:     &groupID,
	}
	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)
	require.True(t, result.Stream)
	require.Len(t, upstream.requests, 4)
	require.Contains(t, string(upstream.bodies[1]), `"chat_session_id":"old_session"`)
	require.Contains(t, string(upstream.bodies[3]), `"chat_session_id":"new_session"`)

	binding, ok := svc.onyxSessions.Get("11:1:sess_retry")
	require.True(t, ok)
	require.Equal(t, "new_session", binding.ChatSessionID)
	require.Equal(t, int64(501), binding.ParentMessageID)
}

func TestWebGatewayOnyxSessionAwareWithoutSessionHashStaysStateless(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:     body,
		Model:    "coder",
		Stream:   true,
		Messages: []any{map[string]any{"role": "user", "content": "hi"}},
	}
	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)
	require.True(t, result.Stream)
	require.Len(t, upstream.requests, 1)
	require.NotContains(t, string(upstream.bodies[0]), `"chat_session_id"`)
	require.NotContains(t, string(upstream.bodies[0]), `"parent_message_id"`)
	_, ok := svc.onyxSessions.Get("0:1:")
	require.False(t, ok)
}

func TestWebGatewayOnyxSessionAwareSharedStoreHitRehydratesLocalCache(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(13)
	shared := newFakeOnyxSessionStore()
	shared.bindings[shared.key(groupID, 1, "sess_shared")] = OnyxSessionBinding{
		ChatSessionID:              "shared_session",
		ParentMessageID:            41,
		ReservedAssistantMessageID: 41,
	}
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":42,\"user_message_id\":41}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello from shared\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, shared)
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      true,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_shared",
		GroupID:     &groupID,
	}
	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)
	require.True(t, result.Stream)
	require.Len(t, upstream.requests, 1)
	require.Contains(t, string(upstream.bodies[0]), `"chat_session_id":"shared_session"`)
	require.Contains(t, string(upstream.bodies[0]), `"parent_message_id":41`)
	require.Equal(t, 1, shared.getCalls)

	binding, ok := svc.onyxSessions.Get("13:1:sess_shared")
	require.True(t, ok)
	require.Equal(t, "shared_session", binding.ChatSessionID)
	require.Equal(t, int64(42), binding.ParentMessageID)
}

func TestWebGatewayOnyxSessionAwareSharedStoreContinuesAcrossInstances(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(15)
	shared := newFakeOnyxSessionStore()
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)

	upstreamA := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"cross_session"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":301,\"user_message_id\":300}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svcA := NewWebGatewayService(upstreamA, nil, nil, nil, shared)
	recA := httptest.NewRecorder()
	cA, _ := gin.CreateTestContext(recA)
	cA.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	parsedA := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      true,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_cross",
		GroupID:     &groupID,
	}
	_, err := svcA.ForwardAsChatCompletions(context.Background(), cA, account, body, parsedA)
	require.NoError(t, err)

	upstreamB := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":302,\"user_message_id\":301}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"followup\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svcB := NewWebGatewayService(upstreamB, nil, nil, nil, shared)
	recB := httptest.NewRecorder()
	cB, _ := gin.CreateTestContext(recB)
	cB.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	parsedB := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      true,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_cross",
		GroupID:     &groupID,
	}
	_, err = svcB.ForwardAsChatCompletions(context.Background(), cB, account, body, parsedB)
	require.NoError(t, err)
	require.Len(t, upstreamB.requests, 1)
	require.Contains(t, string(upstreamB.bodies[0]), `"chat_session_id":"cross_session"`)
	require.Contains(t, string(upstreamB.bodies[0]), `"parent_message_id":301`)

	binding, err := shared.Get(context.Background(), groupID, 1, "sess_cross")
	require.NoError(t, err)
	require.Equal(t, int64(302), binding.ParentMessageID)
}

func TestWebGatewayOnyxSessionAwareSharedStoreSetFailureDoesNotBreakResponse(t *testing.T) {
	resetOnyxSessionMetricsTestState()
	gin.SetMode(gin.TestMode)
	groupID := int64(17)
	shared := newFakeOnyxSessionStore()
	shared.setErr = errors.New("redis write failed")
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"session_set_fail"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":401,\"user_message_id\":400}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, shared)
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      true,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_set_fail",
		GroupID:     &groupID,
	}
	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)
	require.True(t, result.Stream)
	require.GreaterOrEqual(t, shared.setCalls, 1)

	binding, ok := svc.onyxSessions.Get("17:1:sess_set_fail")
	require.True(t, ok)
	require.Equal(t, "session_set_fail", binding.ChatSessionID)
	require.Equal(t, int64(401), binding.ParentMessageID)

	metrics := SnapshotOnyxSessionMetrics()
	require.GreaterOrEqual(t, metrics.SharedStoreSetErrorTotal, int64(1))
	require.GreaterOrEqual(t, metrics.SharedStoreDegradedWriteTotal, int64(1))
}

func TestWebGatewayOnyxSessionMetricsStatelessBypass(t *testing.T) {
	resetOnyxSessionMetricsTestState()
	gin.SetMode(gin.TestMode)
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:     body,
		Model:    "coder",
		Stream:   true,
		Messages: []any{map[string]any{"role": "user", "content": "hi"}},
	}
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)

	metrics := SnapshotOnyxSessionMetrics()
	require.Equal(t, int64(1), metrics.StatelessBypassTotal)
}

func TestWebGatewayOnyxSessionMetricsLocalHotHit(t *testing.T) {
	resetOnyxSessionMetricsTestState()
	gin.SetMode(gin.TestMode)
	groupID := int64(21)
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":52,\"user_message_id\":51}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	svc.onyxSessions.Upsert("21:1:sess_local", OnyxSessionBinding{
		ChatSessionID:              "local_session",
		ParentMessageID:            51,
		ReservedAssistantMessageID: 51,
	})
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      true,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_local",
		GroupID:     &groupID,
	}
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)

	metrics := SnapshotOnyxSessionMetrics()
	require.Equal(t, int64(1), metrics.LocalHotHitTotal)
	require.Equal(t, int64(0), metrics.SharedStoreHitTotal)
	require.Equal(t, int64(0), metrics.SharedStoreMissTotal)
}

func TestWebGatewayOnyxSessionMetricsSharedStoreHitAndReadDegraded(t *testing.T) {
	resetOnyxSessionMetricsTestState()
	gin.SetMode(gin.TestMode)
	groupID := int64(23)
	shared := newFakeOnyxSessionStore()
	shared.bindings[shared.key(groupID, 1, "sess_shared_metrics")] = OnyxSessionBinding{
		ChatSessionID:              "shared_session_metrics",
		ParentMessageID:            61,
		ReservedAssistantMessageID: 61,
	}
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":62,\"user_message_id\":61}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, shared)
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      true,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_shared_metrics",
		GroupID:     &groupID,
	}
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)

	metrics := SnapshotOnyxSessionMetrics()
	require.Equal(t, int64(1), metrics.SharedStoreHitTotal)

	resetOnyxSessionMetricsTestState()
	sharedFail := newFakeOnyxSessionStore()
	sharedFail.getErr = errors.New("redis down")
	upstreamFail := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"degraded_session"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":71,\"user_message_id\":70}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svcFail := NewWebGatewayService(upstreamFail, nil, nil, nil, sharedFail)
	recFail := httptest.NewRecorder()
	cFail, _ := gin.CreateTestContext(recFail)
	cFail.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsedFail := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      true,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_shared_metrics_fail",
		GroupID:     &groupID,
	}
	_, err = svcFail.ForwardAsChatCompletions(context.Background(), cFail, account, body, parsedFail)
	require.NoError(t, err)

	metrics = SnapshotOnyxSessionMetrics()
	require.Equal(t, int64(1), metrics.SharedStoreGetErrorTotal)
	require.Equal(t, int64(1), metrics.SharedStoreDegradedReadTotal)
}

func TestWebGatewayOnyxSessionMetricsCreateAndStreamingAdvance(t *testing.T) {
	resetOnyxSessionMetricsTestState()
	gin.SetMode(gin.TestMode)
	groupID := int64(25)
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"create_metric_session"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":81,\"user_message_id\":80}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      true,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_create_metrics",
		GroupID:     &groupID,
	}
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)

	metrics := SnapshotOnyxSessionMetrics()
	require.Equal(t, int64(1), metrics.CreateChatSessionTotal)
	require.Equal(t, int64(1), metrics.CreateChatSessionSuccessTotal)
	require.Equal(t, int64(1), metrics.StreamingParentAdvanceTotal)
}

func TestWebGatewayOnyxSessionMetricsNonstreamRefresh(t *testing.T) {
	resetOnyxSessionMetricsTestState()
	gin.SetMode(gin.TestMode)
	groupID := int64(27)
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"nonstream_session"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"nonstream_session","answer":"hello"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"nonstream_session","messages":[{"message_type":"assistant","message_id":99}]}`},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, nil)
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":false,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      false,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_nonstream_metrics",
		GroupID:     &groupID,
	}
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)

	metrics := SnapshotOnyxSessionMetrics()
	require.Equal(t, int64(1), metrics.NonstreamParentRefreshTotal)
	require.Equal(t, int64(1), metrics.NonstreamParentRefreshSuccessTotal)
}

func TestWebGatewayOnyxSessionMetricsInvalidSessionRecoveryAndDeleteDegraded(t *testing.T) {
	resetOnyxSessionMetricsTestState()
	gin.SetMode(gin.TestMode)
	groupID := int64(29)
	shared := newFakeOnyxSessionStore()
	shared.delErr = errors.New("redis delete failed")
	upstream := &webGatewayTestUpstream{
		responses: []webGatewayTestResponse{
			{status: http.StatusBadRequest, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"error":"invalid session"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"application/json"}}, body: `{"chat_session_id":"recovered_session"}`},
			{status: http.StatusOK, header: http.Header{"Content-Type": []string{"text/event-stream"}}, body: "data: {\"reserved_assistant_message_id\":121,\"user_message_id\":120}\n\ndata: {\"obj\":{\"type\":\"message_delta\",\"content\":\"hello\"}}\n\ndata: [DONE]\n\n"},
		},
	}
	svc := NewWebGatewayService(upstream, nil, nil, nil, shared)
	svc.onyxSessions.Upsert("29:1:sess_recover_metrics", OnyxSessionBinding{
		ChatSessionID:              "invalid_session",
		ParentMessageID:            111,
		ReservedAssistantMessageID: 111,
	})
	account := webGatewayTestOnyxAccount()
	body := []byte(`{"model":"coder","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	parsed := &ParsedRequest{
		Body:        body,
		Model:       "coder",
		Stream:      true,
		Messages:    []any{map[string]any{"role": "user", "content": "hi"}},
		SessionHash: "sess_recover_metrics",
		GroupID:     &groupID,
	}
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, parsed)
	require.NoError(t, err)

	metrics := SnapshotOnyxSessionMetrics()
	require.Equal(t, int64(1), metrics.InvalidSessionRetryTotal)
	require.Equal(t, int64(1), metrics.InvalidSessionRetrySuccessTotal)
	require.Equal(t, int64(1), metrics.SharedStoreDeleteErrorTotal)
}

func webGatewayTestAccount(creds map[string]any) *Account {
	return &Account{
		ID:          1,
		Name:        "web-test",
		Platform:    PlatformWeb,
		Type:        AccountTypeUpstream,
		Status:      StatusActive,
		Credentials: creds,
	}
}

func webGatewayTestOnyxAccount() *Account {
	return webGatewayTestAccount(map[string]any{
		"base_url":                "https://cloud.onyx.app/api/chat/send-chat-message",
		"request_template":        `{"message":"{{prompt}}","stream":{{stream}},"chat_session_info":{"persona_id":0},"origin":"api"}`,
		"stream_request_template": `{"message":"{{prompt}}","stream":true,"chat_session_info":{"persona_id":0},"origin":"api"}`,
		"supports_stream":         true,
		"upstream_format":         "custom_json",
		"response": map[string]any{
			"text_path": "answer",
			"stream": map[string]any{
				"format": "onyx_chat_sse",
			},
		},
	})
}

func webGatewayOpenAIChatResponseConfig() map[string]any {
	return map[string]any{
		"text_path":        "choices.0.message.content",
		"request_id_path":  "id",
		"model_path":       "model",
		"stop_reason_path": "choices.0.finish_reason",
		"tool_calls": map[string]any{
			"path":             "choices.0.message.tool_calls",
			"id_path":          "id",
			"name_path":        "function.name",
			"arguments_path":   "function.arguments",
			"arguments_format": "json_string",
		},
		"usage_paths": map[string]any{
			"input_tokens":  "usage.prompt_tokens",
			"output_tokens": "usage.completion_tokens",
		},
	}
}

func jsonPathString(body []byte, path string) string {
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return ""
	}
	cur := v
	for _, part := range strings.Split(path, ".") {
		switch node := cur.(type) {
		case map[string]any:
			cur = node[part]
		case []any:
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(node) {
				return ""
			}
			cur = node[i]
		default:
			return ""
		}
	}
	if s, ok := cur.(string); ok {
		return s
	}
	return ""
}

func jsonPathBool(body []byte, path string) bool {
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return false
	}
	if m, ok := v.(map[string]any); ok {
		if b, ok := m[path].(bool); ok {
			return b
		}
	}
	return false
}

func gjsonValidObjectPath(body []byte, path string) bool {
	var v map[string]any
	if err := json.Unmarshal(body, &v); err != nil {
		return false
	}
	_, ok := v[path]
	return ok
}
