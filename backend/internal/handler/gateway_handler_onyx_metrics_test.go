package handler

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestMaybeLogOnyxSessionMetrics(t *testing.T) {
	logSink, restore := captureHandlerStructuredLog(t)
	defer restore()

	gatewayOnyxSessionMetricsLogCounter.Store(gatewayCompatibilityMetricsLogInterval - 1)
	defer gatewayOnyxSessionMetricsLogCounter.Store(0)

	reqLog := logger.L().Named("gateway-handler-test")
	h := &GatewayHandler{}
	serviceMetricsBefore := service.SnapshotOnyxSessionMetrics()

	h.maybeLogOnyxSessionMetrics(reqLog)

	require.True(t, logSink.ContainsMessageAtLevel("gateway.onyx_session_metrics", "info"))
	require.True(t, logSink.ContainsFieldValue("local_hot_hit_total", "0"))
	require.True(t, logSink.ContainsFieldValue("shared_store_hit_rate", "0"))
	require.True(t, logSink.ContainsFieldValue("invalid_session_retry_success_rate", "0"))
	require.Equal(t, serviceMetricsBefore, service.SnapshotOnyxSessionMetrics())
}
