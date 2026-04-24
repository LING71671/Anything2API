package admin

import (
	"errors"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type PlatformHandler struct {
	platformService *service.PlatformService
	oauthService    *service.WebSourceOAuthService
}

func NewPlatformHandler(platformService *service.PlatformService, oauthService *service.WebSourceOAuthService) *PlatformHandler {
	return &PlatformHandler{platformService: platformService, oauthService: oauthService}
}

func (h *PlatformHandler) ListPlatforms(c *gin.Context) {
	includeDisabled := c.Query("include_disabled") == "true"
	defs, err := h.platformService.ListDefinitions(c.Request.Context(), includeDisabled)
	if err != nil {
		response.InternalError(c, err.Error())
		return
	}
	response.Success(c, defs)
}

func (h *PlatformHandler) GetPlatform(c *gin.Context) {
	def, err := h.platformService.GetDefinition(c.Request.Context(), c.Param("platform"))
	if err != nil {
		response.NotFound(c, "Platform not found")
		return
	}
	response.Success(c, def)
}

func (h *PlatformHandler) ListWebSourcePlatforms(c *gin.Context) {
	includeDisabled := c.Query("include_disabled") == "true"
	items, err := h.platformService.ListWebSourcePlatforms(c.Request.Context(), includeDisabled)
	if err != nil {
		response.InternalError(c, err.Error())
		return
	}
	response.Success(c, items)
}

func (h *PlatformHandler) CreateWebSourcePlatform(c *gin.Context) {
	var req service.CreateWebSourcePlatformInput
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	created, err := h.platformService.CreateWebSourcePlatform(c.Request.Context(), req)
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, created)
}

func (h *PlatformHandler) UpdateWebSourcePlatform(c *gin.Context) {
	var req service.UpdateWebSourcePlatformInput
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	updated, err := h.platformService.UpdateWebSourcePlatform(c.Request.Context(), c.Param("platform"), req)
	if err != nil {
		if errors.Is(err, service.ErrPlatformNotFound) {
			response.NotFound(c, "Platform not found")
			return
		}
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, updated)
}

func (h *PlatformHandler) DisableWebSourcePlatform(c *gin.Context) {
	if err := h.platformService.DisableWebSourcePlatform(c.Request.Context(), c.Param("platform")); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, gin.H{"message": "Platform disabled"})
}

func (h *PlatformHandler) DeleteWebSourcePlatform(c *gin.Context) {
	if err := h.platformService.DeleteWebSourcePlatform(c.Request.Context(), c.Param("platform")); err != nil {
		if errors.Is(err, service.ErrPlatformNotFound) {
			response.NotFound(c, "Platform not found")
			return
		}
		if errors.Is(err, service.ErrWebSourcePlatformUsed) {
			response.BadRequest(c, "Platform has accounts, groups, or channel configuration and cannot be deleted")
			return
		}
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, gin.H{"message": "Platform deleted"})
}

type webSourceAuthURLRequest struct {
	RedirectURI string `json:"redirect_uri"`
}

func (h *PlatformHandler) GenerateWebSourceAuthURL(c *gin.Context) {
	var req webSourceAuthURLRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	redirectURI := strings.TrimSpace(req.RedirectURI)
	if redirectURI == "" {
		redirectURI = deriveAdminRedirectURI(c)
	}
	result, err := h.oauthService.GenerateAuthURL(c.Request.Context(), c.Param("platform"), redirectURI)
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, result)
}

type webSourceExchangeCodeRequest struct {
	Code         string `json:"code" binding:"required"`
	RedirectURI  string `json:"redirect_uri" binding:"required"`
	CodeVerifier string `json:"code_verifier" binding:"required"`
}

func (h *PlatformHandler) ExchangeWebSourceCode(c *gin.Context) {
	var req webSourceExchangeCodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	result, err := h.oauthService.ExchangeCode(c.Request.Context(), service.WebSourceExchangeCodeInput{
		Platform:     c.Param("platform"),
		Code:         req.Code,
		RedirectURI:  req.RedirectURI,
		CodeVerifier: req.CodeVerifier,
	})
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, result)
}

func (h *PlatformHandler) RefreshWebSourceAccount(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "Invalid account id")
		return
	}
	result, err := h.oauthService.RefreshAccount(c.Request.Context(), c.Param("platform"), id)
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, result)
}

func deriveAdminRedirectURI(c *gin.Context) string {
	origin := strings.TrimSpace(c.GetHeader("Origin"))
	if origin != "" {
		return strings.TrimRight(origin, "/") + "/auth/callback"
	}
	scheme := "http"
	if c.Request.TLS != nil {
		scheme = "https"
	}
	if xfProto := strings.TrimSpace(c.GetHeader("X-Forwarded-Proto")); xfProto != "" {
		scheme = strings.TrimSpace(strings.Split(xfProto, ",")[0])
	}
	host := strings.TrimSpace(c.Request.Host)
	if host == "" {
		host = "localhost"
	}
	return scheme + "://" + host + "/auth/callback"
}
