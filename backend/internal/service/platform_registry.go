package service

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	PlatformFamilyModel = "model"
	PlatformFamilyWeb   = "web"

	PlatformAdapterNative           = "native"
	PlatformAdapterCustomHTTPJSON   = "custom_http_json"
	PlatformAdapterDynamicHTTPOAuth = "dynamic_http_oauth"

	WebAuthModeNone       = "none"
	WebAuthModeBearer     = "bearer"
	WebAuthModeHeader     = "header"
	WebAuthModeCookie     = "cookie"
	WebAuthModeOAuth2PKCE = "oauth2_pkce"
)

var (
	ErrPlatformNotFound      = errors.New("platform not found")
	ErrWebSourcePlatformUsed = errors.New("web source platform is in use")

	platformKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{1,49}$`)
)

// PlatformDefinition is the admin-visible capability model for one platform key.
type PlatformDefinition struct {
	Key                    string               `json:"key"`
	Label                  string               `json:"label"`
	Family                 string               `json:"family"`
	Status                 string               `json:"status"`
	Icon                   string               `json:"icon"`
	Color                  string               `json:"color"`
	AccountTypes           []string             `json:"account_types"`
	AuthModes              []string             `json:"auth_modes"`
	GatewayAdapter         string               `json:"gateway_adapter"`
	SupportsGroups         bool                 `json:"supports_groups"`
	SupportsChannels       bool                 `json:"supports_channels"`
	SupportsModelMapping   bool                 `json:"supports_model_mapping"`
	SupportsPricing        bool                 `json:"supports_pricing"`
	SupportsOAuthLogin     bool                 `json:"supports_oauth_login"`
	Capabilities           PlatformCapabilities `json:"capabilities"`
	Dynamic                bool                 `json:"dynamic"`
	DefaultBillingMode     string               `json:"default_billing_mode,omitempty"`
	DefaultPerRequestPrice *float64             `json:"default_per_request_price,omitempty"`
	DefaultModelMapping    map[string]any       `json:"default_model_mapping,omitempty"`
	LoginURL               string               `json:"login_url,omitempty"`
}

type PlatformCapabilities struct {
	SupportsStream             bool     `json:"supports_stream"`
	SupportsTools              bool     `json:"supports_tools"`
	SupportsToolResults        bool     `json:"supports_tool_results"`
	SupportsJSONResponseFormat bool     `json:"supports_json_response_format"`
	Tools                      []string `json:"tools,omitempty"`
}

type WebSourcePlatform struct {
	ID                     int64          `json:"id"`
	PlatformKey            string         `json:"platform_key"`
	DisplayName            string         `json:"display_name"`
	Description            string         `json:"description"`
	Status                 string         `json:"status"`
	Icon                   string         `json:"icon"`
	Color                  string         `json:"color"`
	AuthMode               string         `json:"auth_mode"`
	OAuthConfig            map[string]any `json:"oauth_config"`
	RequestConfig          map[string]any `json:"request_config"`
	ResponseConfig         map[string]any `json:"response_config"`
	DefaultModelMapping    map[string]any `json:"default_model_mapping"`
	DefaultBillingMode     string         `json:"default_billing_mode"`
	DefaultPerRequestPrice *float64       `json:"default_per_request_price"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

type CreateWebSourcePlatformInput struct {
	PlatformKey            string         `json:"platform_key"`
	DisplayName            string         `json:"display_name"`
	Description            string         `json:"description"`
	Status                 string         `json:"status"`
	Icon                   string         `json:"icon"`
	Color                  string         `json:"color"`
	AuthMode               string         `json:"auth_mode"`
	OAuthConfig            map[string]any `json:"oauth_config"`
	RequestConfig          map[string]any `json:"request_config"`
	ResponseConfig         map[string]any `json:"response_config"`
	DefaultModelMapping    map[string]any `json:"default_model_mapping"`
	DefaultBillingMode     string         `json:"default_billing_mode"`
	DefaultPerRequestPrice *float64       `json:"default_per_request_price"`
}

type UpdateWebSourcePlatformInput struct {
	DisplayName            *string        `json:"display_name"`
	Description            *string        `json:"description"`
	Status                 *string        `json:"status"`
	Icon                   *string        `json:"icon"`
	Color                  *string        `json:"color"`
	AuthMode               *string        `json:"auth_mode"`
	OAuthConfig            map[string]any `json:"oauth_config"`
	RequestConfig          map[string]any `json:"request_config"`
	ResponseConfig         map[string]any `json:"response_config"`
	DefaultModelMapping    map[string]any `json:"default_model_mapping"`
	DefaultBillingMode     *string        `json:"default_billing_mode"`
	DefaultPerRequestPrice *float64       `json:"default_per_request_price"`
}

type WebSourcePlatformRepository interface {
	List(ctx context.Context, includeDisabled bool) ([]WebSourcePlatform, error)
	GetByKey(ctx context.Context, platformKey string) (*WebSourcePlatform, error)
	Create(ctx context.Context, platform *WebSourcePlatform) error
	Update(ctx context.Context, platform *WebSourcePlatform) error
	Delete(ctx context.Context, platformKey string) error
	HasAccountsOrGroups(ctx context.Context, platformKey string) (bool, error)
}

type PlatformService struct {
	repo WebSourcePlatformRepository
}

func NewPlatformService(repo WebSourcePlatformRepository) *PlatformService {
	return &PlatformService{repo: repo}
}

func BuiltinPlatformDefinitions() []PlatformDefinition {
	return []PlatformDefinition{
		builtinPlatform(PlatformAnthropic, "Anthropic", "sparkles", "#8b5cf6", []string{AccountTypeOAuth, AccountTypeSetupToken, AccountTypeAPIKey, AccountTypeBedrock}),
		builtinPlatform(PlatformOpenAI, "OpenAI", "bot", "#10a37f", []string{AccountTypeOAuth, AccountTypeAPIKey}),
		builtinPlatform(PlatformGemini, "Gemini", "gem", "#4285f4", []string{AccountTypeOAuth, AccountTypeAPIKey}),
		builtinPlatform(PlatformAntigravity, "Antigravity", "rocket", "#f97316", []string{AccountTypeOAuth, AccountTypeAPIKey}),
		{
			Key:                  PlatformWeb,
			Label:                "Custom Web HTTP",
			Family:               PlatformFamilyWeb,
			Status:               StatusActive,
			Icon:                 "globe",
			Color:                "#0ea5e9",
			AccountTypes:         []string{AccountTypeUpstream},
			AuthModes:            []string{WebAuthModeNone, WebAuthModeBearer, WebAuthModeHeader},
			GatewayAdapter:       PlatformAdapterCustomHTTPJSON,
			SupportsGroups:       true,
			SupportsChannels:     true,
			SupportsModelMapping: true,
			SupportsPricing:      true,
			Capabilities: PlatformCapabilities{
				SupportsStream:             false,
				SupportsTools:              false,
				SupportsToolResults:        false,
				SupportsJSONResponseFormat: false,
			},
			Dynamic:            false,
			DefaultBillingMode: string(BillingModePerRequest),
		},
	}
}

func DefaultOnyxWebModelMapping() map[string]any {
	return map[string]any{
		"web-default":       "openai/gpt5.4",
		"gpt5.4":            "openai/gpt5.4",
		"gpt-5.4":           "openai/gpt5.4",
		"gpt5.2":            "openai/gpt5.2",
		"gpt-5.2":           "openai/gpt5.2",
		"generate_image":    "openai/gpt5.4",
		"generate-image":    "openai/gpt5.4",
		"image":             "openai/gpt5.4",
		"image-generation":  "openai/gpt5.4",
		"opus4.7":           "anthropic/claude-opus-4.7",
		"opus-4.7":          "anthropic/claude-opus-4.7",
		"claude-opus-4.7":   "anthropic/claude-opus-4.7",
		"opus4.6":           "anthropic/claude-opus-4.6",
		"opus-4.6":          "anthropic/claude-opus-4.6",
		"claude-opus-4.6":   "anthropic/claude-opus-4.6",
		"sonnet4.6":         "anthropic/claude-sonnet-4.6",
		"sonnet-4.6":        "anthropic/claude-sonnet-4.6",
		"claude-sonnet-4.6": "anthropic/claude-sonnet-4.6",
		"sonnet4.5":         "anthropic/claude-sonnet-4.5",
		"sonnet-4.5":        "anthropic/claude-sonnet-4.5",
		"claude-sonnet-4.5": "anthropic/claude-sonnet-4.5",
	}
}

func defaultWebSourceModelMapping(platformKey string) map[string]any {
	if platformKey == "onyx_web" {
		return DefaultOnyxWebModelMapping()
	}
	return nil
}

func builtinPlatform(key, label, icon, color string, accountTypes []string) PlatformDefinition {
	return PlatformDefinition{
		Key:                  key,
		Label:                label,
		Family:               PlatformFamilyModel,
		Status:               StatusActive,
		Icon:                 icon,
		Color:                color,
		AccountTypes:         accountTypes,
		AuthModes:            []string{},
		GatewayAdapter:       PlatformAdapterNative,
		SupportsGroups:       true,
		SupportsChannels:     true,
		SupportsModelMapping: true,
		SupportsPricing:      true,
		Capabilities: PlatformCapabilities{
			SupportsStream:             true,
			SupportsTools:              true,
			SupportsToolResults:        true,
			SupportsJSONResponseFormat: true,
		},
		Dynamic: false,
	}
}

func (s *PlatformService) ListDefinitions(ctx context.Context, includeDisabled bool) ([]PlatformDefinition, error) {
	defs := BuiltinPlatformDefinitions()
	if s != nil && s.repo != nil {
		rows, err := s.repo.List(ctx, includeDisabled)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			defs = append(defs, webSourceDefinition(row))
		}
	}
	sort.SliceStable(defs, func(i, j int) bool {
		if defs[i].Dynamic != defs[j].Dynamic {
			return !defs[i].Dynamic
		}
		return defs[i].Key < defs[j].Key
	})
	return defs, nil
}

func (s *PlatformService) GetDefinition(ctx context.Context, platform string) (*PlatformDefinition, error) {
	platform = strings.TrimSpace(platform)
	for _, def := range BuiltinPlatformDefinitions() {
		if def.Key == platform {
			cp := def
			return &cp, nil
		}
	}
	if s == nil || s.repo == nil {
		return nil, ErrPlatformNotFound
	}
	row, err := s.repo.GetByKey(ctx, platform)
	if err != nil {
		return nil, err
	}
	if row.Status != StatusActive {
		return nil, ErrPlatformNotFound
	}
	def := webSourceDefinition(*row)
	return &def, nil
}

func (s *PlatformService) IsKnownPlatform(ctx context.Context, platform string) bool {
	_, err := s.GetDefinition(ctx, platform)
	return err == nil
}

func (s *PlatformService) IsWebSourcePlatform(ctx context.Context, platform string) bool {
	def, err := s.GetDefinition(ctx, platform)
	return err == nil && def.Family == PlatformFamilyWeb
}

func (s *PlatformService) ListPlatformKeys(ctx context.Context) []string {
	defs, err := s.ListDefinitions(ctx, false)
	if err != nil {
		return []string{PlatformAnthropic, PlatformGemini, PlatformOpenAI, PlatformAntigravity, PlatformWeb}
	}
	out := make([]string, 0, len(defs))
	for _, def := range defs {
		out = append(out, def.Key)
	}
	return out
}

func (s *PlatformService) ListWebSourcePlatforms(ctx context.Context, includeDisabled bool) ([]WebSourcePlatform, error) {
	if s == nil || s.repo == nil {
		return []WebSourcePlatform{}, nil
	}
	return s.repo.List(ctx, includeDisabled)
}

func (s *PlatformService) GetWebSourcePlatform(ctx context.Context, platformKey string) (*WebSourcePlatform, error) {
	if s == nil || s.repo == nil {
		return nil, ErrPlatformNotFound
	}
	return s.repo.GetByKey(ctx, platformKey)
}

func (s *PlatformService) CreateWebSourcePlatform(ctx context.Context, input CreateWebSourcePlatformInput) (*WebSourcePlatform, error) {
	p := &WebSourcePlatform{
		PlatformKey:            strings.TrimSpace(input.PlatformKey),
		DisplayName:            strings.TrimSpace(input.DisplayName),
		Description:            strings.TrimSpace(input.Description),
		Status:                 normalizeStatus(input.Status),
		Icon:                   strings.TrimSpace(input.Icon),
		Color:                  strings.TrimSpace(input.Color),
		AuthMode:               normalizeWebAuthMode(input.AuthMode),
		OAuthConfig:            cloneMap(input.OAuthConfig),
		RequestConfig:          cloneMap(input.RequestConfig),
		ResponseConfig:         cloneMap(input.ResponseConfig),
		DefaultModelMapping:    cloneMap(input.DefaultModelMapping),
		DefaultBillingMode:     normalizeBillingMode(input.DefaultBillingMode),
		DefaultPerRequestPrice: input.DefaultPerRequestPrice,
	}
	if p.Icon == "" {
		p.Icon = "globe"
	}
	if p.Color == "" {
		p.Color = "#0ea5e9"
	}
	if err := validateWebSourcePlatform(p, true); err != nil {
		return nil, err
	}
	if isBuiltinPlatformKey(p.PlatformKey) {
		return nil, fmt.Errorf("platform_key %q is reserved", p.PlatformKey)
	}
	if s == nil || s.repo == nil {
		return nil, errors.New("web source platform repository is not configured")
	}
	if err := s.repo.Create(ctx, p); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *PlatformService) UpdateWebSourcePlatform(ctx context.Context, platformKey string, input UpdateWebSourcePlatformInput) (*WebSourcePlatform, error) {
	if s == nil || s.repo == nil {
		return nil, errors.New("web source platform repository is not configured")
	}
	p, err := s.repo.GetByKey(ctx, platformKey)
	if err != nil {
		return nil, err
	}
	if input.DisplayName != nil {
		p.DisplayName = strings.TrimSpace(*input.DisplayName)
	}
	if input.Description != nil {
		p.Description = strings.TrimSpace(*input.Description)
	}
	if input.Status != nil {
		p.Status = normalizeStatus(*input.Status)
	}
	if input.Icon != nil {
		p.Icon = strings.TrimSpace(*input.Icon)
	}
	if input.Color != nil {
		p.Color = strings.TrimSpace(*input.Color)
	}
	if input.AuthMode != nil {
		p.AuthMode = normalizeWebAuthMode(*input.AuthMode)
	}
	if input.OAuthConfig != nil {
		p.OAuthConfig = cloneMap(input.OAuthConfig)
	}
	if input.RequestConfig != nil {
		p.RequestConfig = cloneMap(input.RequestConfig)
	}
	if input.ResponseConfig != nil {
		p.ResponseConfig = cloneMap(input.ResponseConfig)
	}
	if input.DefaultModelMapping != nil {
		p.DefaultModelMapping = cloneMap(input.DefaultModelMapping)
	}
	if input.DefaultBillingMode != nil {
		p.DefaultBillingMode = normalizeBillingMode(*input.DefaultBillingMode)
	}
	if input.DefaultPerRequestPrice != nil {
		p.DefaultPerRequestPrice = input.DefaultPerRequestPrice
	}
	if err := validateWebSourcePlatform(p, false); err != nil {
		return nil, err
	}
	if err := s.repo.Update(ctx, p); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *PlatformService) DisableWebSourcePlatform(ctx context.Context, platformKey string) error {
	if s == nil || s.repo == nil {
		return errors.New("web source platform repository is not configured")
	}
	status := StatusDisabled
	_, err := s.UpdateWebSourcePlatform(ctx, platformKey, UpdateWebSourcePlatformInput{Status: &status})
	return err
}

func (s *PlatformService) DeleteWebSourcePlatform(ctx context.Context, platformKey string) error {
	if s == nil || s.repo == nil {
		return errors.New("web source platform repository is not configured")
	}
	platformKey = strings.TrimSpace(platformKey)
	if isBuiltinPlatformKey(platformKey) {
		return fmt.Errorf("platform_key %q is reserved", platformKey)
	}
	used, err := s.repo.HasAccountsOrGroups(ctx, platformKey)
	if err != nil {
		return err
	}
	if used {
		return ErrWebSourcePlatformUsed
	}
	return s.repo.Delete(ctx, platformKey)
}

func webSourceDefinition(row WebSourcePlatform) PlatformDefinition {
	authModes := []string{row.AuthMode}
	if row.AuthMode == WebAuthModeOAuth2PKCE {
		authModes = []string{WebAuthModeOAuth2PKCE, WebAuthModeBearer}
	}
	return PlatformDefinition{
		Key:                    row.PlatformKey,
		Label:                  row.DisplayName,
		Family:                 PlatformFamilyWeb,
		Status:                 row.Status,
		Icon:                   row.Icon,
		Color:                  row.Color,
		AccountTypes:           []string{AccountTypeOAuth, AccountTypeUpstream},
		AuthModes:              authModes,
		GatewayAdapter:         PlatformAdapterDynamicHTTPOAuth,
		SupportsGroups:         true,
		SupportsChannels:       true,
		SupportsModelMapping:   true,
		SupportsPricing:        true,
		SupportsOAuthLogin:     row.AuthMode == WebAuthModeOAuth2PKCE,
		Capabilities:           webSourceCapabilities(row),
		Dynamic:                true,
		DefaultBillingMode:     row.DefaultBillingMode,
		DefaultPerRequestPrice: row.DefaultPerRequestPrice,
		DefaultModelMapping:    cloneMap(row.DefaultModelMapping),
		LoginURL:               stringMapValue(row.OAuthConfig, "login_url"),
	}
}

func webSourceCapabilities(row WebSourcePlatform) PlatformCapabilities {
	requestTemplate := stringMapValue(row.RequestConfig, "request_template") + " " + stringMapValue(row.RequestConfig, "stream_request_template")
	responseToolCalls := anyMap(row.ResponseConfig["tool_calls"])
	supportsTools := boolMapValue(row.RequestConfig, "supports_tools") ||
		strings.Contains(requestTemplate, "{{tools_json}}") ||
		strings.Contains(requestTemplate, "{{tool_choice_json}}") ||
		strings.TrimSpace(stringMapValue(responseToolCalls, "path")) != ""
	return PlatformCapabilities{
		SupportsStream:             boolMapValue(row.RequestConfig, "supports_stream"),
		SupportsTools:              supportsTools,
		SupportsToolResults:        boolMapValue(row.RequestConfig, "supports_tool_results") || supportsTools,
		SupportsJSONResponseFormat: boolMapValue(row.RequestConfig, "supports_json_response_format") || strings.Contains(requestTemplate, "{{response_format_json}}"),
		Tools:                      stringSliceMapValue(row.RequestConfig, "tool_capabilities"),
	}
}

func validateWebSourcePlatform(p *WebSourcePlatform, requireKey bool) error {
	if p == nil {
		return errors.New("web source platform is required")
	}
	if requireKey && !platformKeyPattern.MatchString(p.PlatformKey) {
		return errors.New("platform_key must be lowercase snake_case and 2-50 chars")
	}
	if requireKey && !strings.HasSuffix(p.PlatformKey, "_web") {
		return errors.New("dynamic web platform_key must end with _web")
	}
	if strings.TrimSpace(p.DisplayName) == "" {
		return errors.New("display_name is required")
	}
	if p.Status != StatusActive && p.Status != StatusDisabled {
		return errors.New("status must be active or disabled")
	}
	if !isAllowedWebAuthMode(p.AuthMode) {
		return errors.New("auth_mode must be one of none, bearer, header, cookie, oauth2_pkce")
	}
	if p.DefaultBillingMode == "" {
		p.DefaultBillingMode = string(BillingModePerRequest)
	}
	if p.DefaultBillingMode != string(BillingModeToken) && p.DefaultBillingMode != string(BillingModePerRequest) && p.DefaultBillingMode != string(BillingModeImage) {
		return errors.New("default_billing_mode must be token, per_request, or image")
	}
	if err := validateDynamicRequestConfig(p.RequestConfig); err != nil {
		return err
	}
	if err := validateDynamicResponseConfig(p.ResponseConfig); err != nil {
		return err
	}
	if p.AuthMode == WebAuthModeOAuth2PKCE {
		if err := validateOAuthConfig(p.OAuthConfig); err != nil {
			return err
		}
	}
	if err := validateOptionalURL(p.OAuthConfig, "login_url"); err != nil {
		return err
	}
	return nil
}

func validateDynamicRequestConfig(cfg map[string]any) error {
	if strings.TrimSpace(stringMapValue(cfg, "base_url")) == "" {
		return errors.New("request_config.base_url is required")
	}
	if strings.TrimSpace(stringMapValue(cfg, "request_template")) == "" {
		return errors.New("request_config.request_template is required")
	}
	return nil
}

func validateDynamicResponseConfig(cfg map[string]any) error {
	if strings.TrimSpace(stringMapValue(cfg, "text_path")) == "" &&
		strings.TrimSpace(stringMapValue(anyMap(cfg["tool_calls"]), "path")) == "" {
		return errors.New("response_config.text_path or response_config.tool_calls.path is required")
	}
	return nil
}

func validateOAuthConfig(cfg map[string]any) error {
	for _, key := range []string{"auth_url", "token_url", "client_id"} {
		raw := strings.TrimSpace(stringMapValue(cfg, key))
		if raw == "" {
			return fmt.Errorf("oauth_config.%s is required", key)
		}
		if strings.HasSuffix(key, "_url") {
			u, err := url.Parse(raw)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return fmt.Errorf("oauth_config.%s must be a valid URL", key)
			}
		}
	}
	return nil
}

func validateOptionalURL(cfg map[string]any, key string) error {
	raw := strings.TrimSpace(stringMapValue(cfg, key))
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("oauth_config.%s must be a valid URL", key)
	}
	return nil
}

func IsWebPlatformKey(platform string) bool {
	platform = strings.TrimSpace(platform)
	return platform == PlatformWeb || strings.HasSuffix(platform, "_web")
}

func isBuiltinPlatformKey(key string) bool {
	for _, def := range BuiltinPlatformDefinitions() {
		if def.Key == key {
			return true
		}
	}
	return false
}

func normalizeWebAuthMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return WebAuthModeNone
	}
	return mode
}

func isAllowedWebAuthMode(mode string) bool {
	switch mode {
	case WebAuthModeNone, WebAuthModeBearer, WebAuthModeHeader, WebAuthModeCookie, WebAuthModeOAuth2PKCE:
		return true
	default:
		return false
	}
}

func normalizeBillingMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return string(BillingModePerRequest)
	}
	return mode
}

func normalizeStatus(status string) string {
	status = strings.ToLower(strings.TrimSpace(status))
	if status == "" {
		return StatusActive
	}
	return status
}

func stringMapValue(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func stringSliceMapValue(m map[string]any, key string) []string {
	if m == nil {
		return nil
	}
	raw, ok := m[key]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case []string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s := strings.TrimSpace(item); s != "" {
				out = append(out, s)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s := strings.TrimSpace(fmt.Sprint(item)); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
