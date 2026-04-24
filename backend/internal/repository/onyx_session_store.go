package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const onyxSessionPrefix = "onyx_session:"

type onyxSessionStore struct {
	rdb *redis.Client
}

func NewOnyxSessionStore(rdb *redis.Client) service.OnyxSessionStore {
	return &onyxSessionStore{rdb: rdb}
}

func buildOnyxSessionKey(groupID, accountID int64, sessionHash string) string {
	return fmt.Sprintf("%s%d:%d:%s", onyxSessionPrefix, groupID, accountID, sessionHash)
}

func (s *onyxSessionStore) Get(ctx context.Context, groupID, accountID int64, sessionHash string) (service.OnyxSessionBinding, error) {
	if s == nil || s.rdb == nil {
		return service.OnyxSessionBinding{}, nil
	}
	sessionHash = strings.TrimSpace(sessionHash)
	if accountID <= 0 || sessionHash == "" {
		return service.OnyxSessionBinding{}, nil
	}
	raw, err := s.rdb.Get(ctx, buildOnyxSessionKey(groupID, accountID, sessionHash)).Bytes()
	if err != nil {
		return service.OnyxSessionBinding{}, err
	}
	var binding service.OnyxSessionBinding
	if err := json.Unmarshal(raw, &binding); err != nil {
		return service.OnyxSessionBinding{}, err
	}
	return binding, nil
}

func (s *onyxSessionStore) Set(ctx context.Context, groupID, accountID int64, sessionHash string, binding service.OnyxSessionBinding, ttl time.Duration) error {
	if s == nil || s.rdb == nil {
		return nil
	}
	sessionHash = strings.TrimSpace(sessionHash)
	if accountID <= 0 || sessionHash == "" {
		return nil
	}
	binding.ChatSessionID = strings.TrimSpace(binding.ChatSessionID)
	payload, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, buildOnyxSessionKey(groupID, accountID, sessionHash), payload, ttl).Err()
}

func (s *onyxSessionStore) Delete(ctx context.Context, groupID, accountID int64, sessionHash string) error {
	if s == nil || s.rdb == nil {
		return nil
	}
	sessionHash = strings.TrimSpace(sessionHash)
	if accountID <= 0 || sessionHash == "" {
		return nil
	}
	return s.rdb.Del(ctx, buildOnyxSessionKey(groupID, accountID, sessionHash)).Err()
}
