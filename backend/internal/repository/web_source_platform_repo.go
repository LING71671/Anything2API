package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type webSourcePlatformRepository struct {
	db *sql.DB
}

func NewWebSourcePlatformRepository(db *sql.DB) service.WebSourcePlatformRepository {
	return &webSourcePlatformRepository{db: db}
}

func (r *webSourcePlatformRepository) List(ctx context.Context, includeDisabled bool) ([]service.WebSourcePlatform, error) {
	query := `SELECT id, platform_key, display_name, COALESCE(description, ''), status, icon, color, auth_mode,
       oauth_config, request_config, response_config, default_model_mapping,
       default_billing_mode, default_per_request_price, created_at, updated_at
       FROM web_source_platforms`
	args := []any{}
	if !includeDisabled {
		query += ` WHERE status = $1`
		args = append(args, service.StatusActive)
	}
	query += ` ORDER BY platform_key ASC`

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list web source platforms: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []service.WebSourcePlatform{}
	for rows.Next() {
		p, err := scanWebSourcePlatform(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *webSourcePlatformRepository) GetByKey(ctx context.Context, platformKey string) (*service.WebSourcePlatform, error) {
	row := r.db.QueryRowContext(ctx, `SELECT id, platform_key, display_name, COALESCE(description, ''), status, icon, color, auth_mode,
       oauth_config, request_config, response_config, default_model_mapping,
       default_billing_mode, default_per_request_price, created_at, updated_at
       FROM web_source_platforms WHERE platform_key = $1`, platformKey)
	p, err := scanWebSourcePlatform(row)
	if err == sql.ErrNoRows {
		return nil, service.ErrPlatformNotFound
	}
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (r *webSourcePlatformRepository) Create(ctx context.Context, p *service.WebSourcePlatform) error {
	oauthJSON, _ := json.Marshal(nonNilMap(p.OAuthConfig))
	requestJSON, _ := json.Marshal(nonNilMap(p.RequestConfig))
	responseJSON, _ := json.Marshal(nonNilMap(p.ResponseConfig))
	mappingJSON, _ := json.Marshal(nonNilMap(p.DefaultModelMapping))
	err := r.db.QueryRowContext(ctx, `INSERT INTO web_source_platforms
       (platform_key, display_name, description, status, icon, color, auth_mode, oauth_config, request_config, response_config, default_model_mapping, default_billing_mode, default_per_request_price)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
       RETURNING id, created_at, updated_at`,
		p.PlatformKey, p.DisplayName, p.Description, p.Status, p.Icon, p.Color, p.AuthMode,
		oauthJSON, requestJSON, responseJSON, mappingJSON, p.DefaultBillingMode, p.DefaultPerRequestPrice,
	).Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("platform_key already exists")
		}
		return fmt.Errorf("create web source platform: %w", err)
	}
	return nil
}

func (r *webSourcePlatformRepository) Update(ctx context.Context, p *service.WebSourcePlatform) error {
	oauthJSON, _ := json.Marshal(nonNilMap(p.OAuthConfig))
	requestJSON, _ := json.Marshal(nonNilMap(p.RequestConfig))
	responseJSON, _ := json.Marshal(nonNilMap(p.ResponseConfig))
	mappingJSON, _ := json.Marshal(nonNilMap(p.DefaultModelMapping))
	err := r.db.QueryRowContext(ctx, `UPDATE web_source_platforms
       SET display_name = $2, description = $3, status = $4, icon = $5, color = $6, auth_mode = $7,
           oauth_config = $8, request_config = $9, response_config = $10, default_model_mapping = $11,
           default_billing_mode = $12, default_per_request_price = $13, updated_at = NOW()
       WHERE platform_key = $1
       RETURNING id, created_at, updated_at`,
		p.PlatformKey, p.DisplayName, p.Description, p.Status, p.Icon, p.Color, p.AuthMode,
		oauthJSON, requestJSON, responseJSON, mappingJSON, p.DefaultBillingMode, p.DefaultPerRequestPrice,
	).Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt)
	if err == sql.ErrNoRows {
		return service.ErrPlatformNotFound
	}
	if err != nil {
		return fmt.Errorf("update web source platform: %w", err)
	}
	return nil
}

func (r *webSourcePlatformRepository) Delete(ctx context.Context, platformKey string) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM web_source_platforms WHERE platform_key = $1`, platformKey)
	if err != nil {
		return fmt.Errorf("delete web source platform: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete web source platform rows affected: %w", err)
	}
	if affected == 0 {
		return service.ErrPlatformNotFound
	}
	return nil
}

func (r *webSourcePlatformRepository) HasAccountsOrGroups(ctx context.Context, platformKey string) (bool, error) {
	var count int
	if err := r.db.QueryRowContext(ctx, `SELECT
       (SELECT COUNT(*) FROM accounts WHERE platform = $1 AND deleted_at IS NULL) +
       (SELECT COUNT(*) FROM groups WHERE platform = $1 AND deleted_at IS NULL) +
       (SELECT COUNT(*) FROM channel_model_pricing WHERE platform = $1) +
       (SELECT COUNT(*) FROM channels WHERE model_mapping ? $1) +
       (SELECT COUNT(*) FROM channel_account_stats_model_pricing WHERE platform = $1)`, platformKey).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanWebSourcePlatform(row scanner) (*service.WebSourcePlatform, error) {
	var p service.WebSourcePlatform
	var oauthJSON, requestJSON, responseJSON, mappingJSON []byte
	var perRequest sql.NullFloat64
	if err := row.Scan(
		&p.ID, &p.PlatformKey, &p.DisplayName, &p.Description, &p.Status, &p.Icon, &p.Color, &p.AuthMode,
		&oauthJSON, &requestJSON, &responseJSON, &mappingJSON,
		&p.DefaultBillingMode, &perRequest, &p.CreatedAt, &p.UpdatedAt,
	); err != nil {
		return nil, err
	}
	p.OAuthConfig = unmarshalAnyMap(oauthJSON)
	p.RequestConfig = unmarshalAnyMap(requestJSON)
	p.ResponseConfig = unmarshalAnyMap(responseJSON)
	p.DefaultModelMapping = unmarshalAnyMap(mappingJSON)
	if perRequest.Valid {
		v := perRequest.Float64
		p.DefaultPerRequestPrice = &v
	}
	return &p, nil
}

func unmarshalAnyMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func nonNilMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}
