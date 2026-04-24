package service

import (
	"context"
	"strings"
	"time"
)

const onyxSessionStoreRedisTimeout = 3 * time.Second

// OnyxSessionBinding stores the upstream Onyx session cursor that lets
// Anything2API continue a prior chat across requests and instances.
type OnyxSessionBinding struct {
	ChatSessionID              string    `json:"chat_session_id,omitempty"`
	ParentMessageID            int64     `json:"parent_message_id,omitempty"`
	ReservedAssistantMessageID int64     `json:"reserved_assistant_message_id,omitempty"`
	UserMessageID              int64     `json:"user_message_id,omitempty"`
	UpdatedAt                  time.Time `json:"updated_at,omitempty"`
	ExpiresAt                  time.Time `json:"expires_at,omitempty"`
}

// OnyxSessionStore persists Onyx session cursors in a shared cache so a later
// request can resume the same upstream conversation even on another instance.
type OnyxSessionStore interface {
	Get(ctx context.Context, groupID, accountID int64, sessionHash string) (OnyxSessionBinding, error)
	Set(ctx context.Context, groupID, accountID int64, sessionHash string, binding OnyxSessionBinding, ttl time.Duration) error
	Delete(ctx context.Context, groupID, accountID int64, sessionHash string) error
}

func normalizeOnyxSessionBinding(binding OnyxSessionBinding) OnyxSessionBinding {
	binding.ChatSessionID = strings.TrimSpace(binding.ChatSessionID)
	return binding
}

func onyxSessionBindingEmpty(binding OnyxSessionBinding) bool {
	binding = normalizeOnyxSessionBinding(binding)
	return binding.ChatSessionID == "" &&
		binding.ParentMessageID <= 0 &&
		binding.ReservedAssistantMessageID <= 0 &&
		binding.UserMessageID <= 0
}

func withOnyxSessionStoreRedisTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, onyxSessionStoreRedisTimeout)
}
