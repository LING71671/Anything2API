package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

type WebSourceOAuthService struct {
	platforms   *PlatformService
	accountRepo AccountRepository
	httpClient  *http.Client
}

func NewWebSourceOAuthService(platforms *PlatformService, accountRepo AccountRepository) *WebSourceOAuthService {
	return &WebSourceOAuthService{
		platforms:   platforms,
		accountRepo: accountRepo,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
	}
}

type WebSourceAuthURLResult struct {
	AuthURL      string `json:"auth_url"`
	State        string `json:"state"`
	CodeVerifier string `json:"code_verifier"`
	RedirectURI  string `json:"redirect_uri"`
}

type WebSourceExchangeCodeInput struct {
	Platform     string
	Code         string
	RedirectURI  string
	CodeVerifier string
}

type WebSourceTokenResult struct {
	Credentials map[string]any `json:"credentials"`
	ExpiresAt   int64          `json:"expires_at,omitempty"`
}

func (s *WebSourceOAuthService) GenerateAuthURL(ctx context.Context, platformKey, redirectURI string) (*WebSourceAuthURLResult, error) {
	platform, err := s.requireOAuthPlatform(ctx, platformKey)
	if err != nil {
		return nil, err
	}
	state := randomURLSafe(24)
	verifier := randomURLSafe(48)
	challenge := pkceChallenge(verifier)

	authURL := strings.TrimSpace(stringMapValue(platform.OAuthConfig, "auth_url"))
	u, err := url.Parse(authURL)
	if err != nil {
		return nil, fmt.Errorf("invalid auth_url: %w", err)
	}
	q := u.Query()
	q.Set("response_type", "code")
	q.Set("client_id", strings.TrimSpace(stringMapValue(platform.OAuthConfig, "client_id")))
	q.Set("redirect_uri", redirectURI)
	q.Set("state", state)
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")
	if scope := strings.TrimSpace(stringMapValue(platform.OAuthConfig, "scope")); scope != "" {
		q.Set("scope", scope)
	}
	for k, v := range anyMap(platform.OAuthConfig["extra_auth_params"]) {
		if s, ok := v.(string); ok && strings.TrimSpace(k) != "" {
			q.Set(k, s)
		}
	}
	u.RawQuery = q.Encode()

	return &WebSourceAuthURLResult{
		AuthURL:      u.String(),
		State:        state,
		CodeVerifier: verifier,
		RedirectURI:  redirectURI,
	}, nil
}

func (s *WebSourceOAuthService) ExchangeCode(ctx context.Context, input WebSourceExchangeCodeInput) (*WebSourceTokenResult, error) {
	platform, err := s.requireOAuthPlatform(ctx, input.Platform)
	if err != nil {
		return nil, err
	}
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", input.Code)
	form.Set("redirect_uri", input.RedirectURI)
	form.Set("client_id", strings.TrimSpace(stringMapValue(platform.OAuthConfig, "client_id")))
	form.Set("code_verifier", input.CodeVerifier)
	return s.exchangeToken(ctx, platform, form, nil)
}

func (s *WebSourceOAuthService) RefreshAccount(ctx context.Context, platformKey string, accountID int64) (*WebSourceTokenResult, error) {
	if s.accountRepo == nil {
		return nil, fmt.Errorf("account repository is not configured")
	}
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if account.Platform != platformKey {
		return nil, fmt.Errorf("account platform mismatch: expected %s, got %s", platformKey, account.Platform)
	}
	platform, err := s.requireOAuthPlatform(ctx, platformKey)
	if err != nil {
		return nil, err
	}
	refreshToken := firstString(account.Credentials, "refresh_token")
	if refreshToken == "" {
		return nil, fmt.Errorf("refresh_token is required")
	}
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	form.Set("client_id", strings.TrimSpace(stringMapValue(platform.OAuthConfig, "client_id")))
	result, err := s.exchangeToken(ctx, platform, form, account.Credentials)
	if err != nil {
		account.Status = StatusError
		account.Schedulable = false
		account.ErrorMessage = err.Error()
		_ = s.accountRepo.Update(ctx, account)
		return nil, err
	}
	account.Credentials = result.Credentials
	account.Status = StatusActive
	account.Schedulable = true
	account.ErrorMessage = ""
	if err := s.accountRepo.Update(ctx, account); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *WebSourceOAuthService) EnsureFreshAccessToken(ctx context.Context, account *Account, refreshWindow time.Duration) error {
	if s == nil || account == nil {
		return nil
	}
	if account.Platform == PlatformWeb || account.Type != AccountTypeOAuth {
		return nil
	}
	platform, err := s.requireOAuthPlatform(ctx, account.Platform)
	if err != nil {
		return nil
	}
	if platform.AuthMode != WebAuthModeOAuth2PKCE {
		return nil
	}
	expiresAt := int64FromAny(account.Credentials["expires_at"])
	if expiresAt <= 0 {
		return nil
	}
	window := refreshWindow
	if window <= 0 {
		window = time.Minute
	}
	if time.Unix(expiresAt, 0).After(time.Now().Add(window)) {
		return nil
	}
	result, err := s.RefreshAccount(ctx, account.Platform, account.ID)
	if err != nil {
		return err
	}
	if result != nil && result.Credentials != nil {
		account.Credentials = result.Credentials
		account.Status = StatusActive
		account.Schedulable = true
		account.ErrorMessage = ""
	}
	return nil
}

func (s *WebSourceOAuthService) requireOAuthPlatform(ctx context.Context, platformKey string) (*WebSourcePlatform, error) {
	if s == nil || s.platforms == nil {
		return nil, fmt.Errorf("platform service is not configured")
	}
	platform, err := s.platforms.GetWebSourcePlatform(ctx, platformKey)
	if err != nil {
		return nil, err
	}
	if platform.Status != StatusActive {
		return nil, fmt.Errorf("platform %s is disabled", platformKey)
	}
	if platform.AuthMode != WebAuthModeOAuth2PKCE {
		return nil, fmt.Errorf("platform %s does not support OAuth2 PKCE", platformKey)
	}
	return platform, nil
}

func (s *WebSourceOAuthService) exchangeToken(ctx context.Context, platform *WebSourcePlatform, form url.Values, existing map[string]any) (*WebSourceTokenResult, error) {
	tokenURL := strings.TrimSpace(stringMapValue(platform.OAuthConfig, "token_url"))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("content-type", "application/x-www-form-urlencoded")
	req.Header.Set("accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("token request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("token endpoint returned %d: %s", resp.StatusCode, sanitizeUpstreamErrorMessage(extractUpstreamErrorMessage(body)))
	}
	if !gjson.ValidBytes(body) {
		return nil, fmt.Errorf("token endpoint returned invalid JSON")
	}

	credentials := cloneMap(existing)
	accessPath := pathOrDefault(platform.OAuthConfig, "access_token_path", "access_token")
	refreshPath := pathOrDefault(platform.OAuthConfig, "refresh_token_path", "refresh_token")
	expiresInPath := pathOrDefault(platform.OAuthConfig, "expires_in_path", "expires_in")
	tokenTypePath := pathOrDefault(platform.OAuthConfig, "token_type_path", "token_type")
	if access := gjson.GetBytes(body, accessPath).String(); strings.TrimSpace(access) != "" {
		credentials["access_token"] = strings.TrimSpace(access)
	}
	if refresh := gjson.GetBytes(body, refreshPath).String(); strings.TrimSpace(refresh) != "" {
		credentials["refresh_token"] = strings.TrimSpace(refresh)
	}
	if tokenType := gjson.GetBytes(body, tokenTypePath).String(); strings.TrimSpace(tokenType) != "" {
		credentials["token_type"] = strings.TrimSpace(tokenType)
	}
	credentials["obtained_at"] = time.Now().Unix()
	if credentials["access_token"] == nil || strings.TrimSpace(fmt.Sprint(credentials["access_token"])) == "" {
		return nil, fmt.Errorf("token response missing access token at %q", accessPath)
	}
	expiresAt := tokenExpiresAt(body, expiresInPath, pathOrDefault(platform.OAuthConfig, "expires_at_path", ""))
	if expiresAt > 0 {
		credentials["expires_at"] = expiresAt
	}
	return &WebSourceTokenResult{Credentials: credentials, ExpiresAt: expiresAt}, nil
}

func tokenExpiresAt(body []byte, expiresInPath, expiresAtPath string) int64 {
	if expiresAtPath != "" {
		res := gjson.GetBytes(body, expiresAtPath)
		if res.Exists() {
			if res.Type == gjson.Number {
				return res.Int()
			}
			if parsed, err := strconv.ParseInt(res.String(), 10, 64); err == nil {
				return parsed
			}
		}
	}
	if expiresInPath != "" {
		res := gjson.GetBytes(body, expiresInPath)
		if res.Exists() {
			secs := res.Int()
			if secs > 0 {
				return time.Now().Add(time.Duration(secs) * time.Second).Unix()
			}
		}
	}
	return 0
}

func int64FromAny(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n
	default:
		return 0
	}
}

func pathOrDefault(m map[string]any, key, fallback string) string {
	if v := strings.TrimSpace(stringMapValue(m, key)); v != "" {
		return v
	}
	return fallback
}

func randomURLSafe(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (r *WebSourceTokenResult) MarshalJSON() ([]byte, error) {
	type alias WebSourceTokenResult
	return json.Marshal((*alias)(r))
}
