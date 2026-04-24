package service

import "sync/atomic"

type OnyxSessionMetricsSnapshot struct {
	LocalHotHitTotal                   int64
	SharedStoreHitTotal                int64
	SharedStoreMissTotal               int64
	StatelessBypassTotal               int64
	SharedStoreGetErrorTotal           int64
	SharedStoreSetErrorTotal           int64
	SharedStoreDeleteErrorTotal        int64
	CreateChatSessionTotal             int64
	CreateChatSessionSuccessTotal      int64
	StreamingParentAdvanceTotal        int64
	NonstreamParentRefreshTotal        int64
	NonstreamParentRefreshSuccessTotal int64
	InvalidSessionRetryTotal           int64
	InvalidSessionRetrySuccessTotal    int64
	SharedStoreDegradedReadTotal       int64
	SharedStoreDegradedWriteTotal      int64

	SharedStoreHitRate             float64
	InvalidSessionRetrySuccessRate float64
}

var (
	onyxSessionLocalHotHitTotal                   atomic.Int64
	onyxSessionSharedStoreHitTotal                atomic.Int64
	onyxSessionSharedStoreMissTotal               atomic.Int64
	onyxSessionStatelessBypassTotal               atomic.Int64
	onyxSessionSharedStoreGetErrorTotal           atomic.Int64
	onyxSessionSharedStoreSetErrorTotal           atomic.Int64
	onyxSessionSharedStoreDeleteErrorTotal        atomic.Int64
	onyxSessionCreateChatSessionTotal             atomic.Int64
	onyxSessionCreateChatSessionSuccessTotal      atomic.Int64
	onyxSessionStreamingParentAdvanceTotal        atomic.Int64
	onyxSessionNonstreamParentRefreshTotal        atomic.Int64
	onyxSessionNonstreamParentRefreshSuccessTotal atomic.Int64
	onyxSessionInvalidSessionRetryTotal           atomic.Int64
	onyxSessionInvalidSessionRetrySuccessTotal    atomic.Int64
	onyxSessionSharedStoreDegradedReadTotal       atomic.Int64
	onyxSessionSharedStoreDegradedWriteTotal      atomic.Int64
)

func recordOnyxSessionLocalHotHit() { onyxSessionLocalHotHitTotal.Add(1) }

func recordOnyxSessionSharedStoreHit() { onyxSessionSharedStoreHitTotal.Add(1) }

func recordOnyxSessionSharedStoreMiss() { onyxSessionSharedStoreMissTotal.Add(1) }

func recordOnyxSessionStatelessBypass() { onyxSessionStatelessBypassTotal.Add(1) }

func recordOnyxSessionSharedStoreGetError() { onyxSessionSharedStoreGetErrorTotal.Add(1) }

func recordOnyxSessionSharedStoreSetError() { onyxSessionSharedStoreSetErrorTotal.Add(1) }

func recordOnyxSessionSharedStoreDeleteError() { onyxSessionSharedStoreDeleteErrorTotal.Add(1) }

func recordOnyxCreateChatSession() { onyxSessionCreateChatSessionTotal.Add(1) }

func recordOnyxCreateChatSessionSuccess() { onyxSessionCreateChatSessionSuccessTotal.Add(1) }

func recordOnyxStreamingParentAdvance() { onyxSessionStreamingParentAdvanceTotal.Add(1) }

func recordOnyxNonstreamParentRefresh() { onyxSessionNonstreamParentRefreshTotal.Add(1) }

func recordOnyxNonstreamParentRefreshSuccess() { onyxSessionNonstreamParentRefreshSuccessTotal.Add(1) }

func recordOnyxInvalidSessionRetry() { onyxSessionInvalidSessionRetryTotal.Add(1) }

func recordOnyxInvalidSessionRetrySuccess() { onyxSessionInvalidSessionRetrySuccessTotal.Add(1) }

func recordOnyxSharedStoreDegradedRead() { onyxSessionSharedStoreDegradedReadTotal.Add(1) }

func recordOnyxSharedStoreDegradedWrite() { onyxSessionSharedStoreDegradedWriteTotal.Add(1) }

func SnapshotOnyxSessionMetrics() OnyxSessionMetricsSnapshot {
	s := OnyxSessionMetricsSnapshot{
		LocalHotHitTotal:                   onyxSessionLocalHotHitTotal.Load(),
		SharedStoreHitTotal:                onyxSessionSharedStoreHitTotal.Load(),
		SharedStoreMissTotal:               onyxSessionSharedStoreMissTotal.Load(),
		StatelessBypassTotal:               onyxSessionStatelessBypassTotal.Load(),
		SharedStoreGetErrorTotal:           onyxSessionSharedStoreGetErrorTotal.Load(),
		SharedStoreSetErrorTotal:           onyxSessionSharedStoreSetErrorTotal.Load(),
		SharedStoreDeleteErrorTotal:        onyxSessionSharedStoreDeleteErrorTotal.Load(),
		CreateChatSessionTotal:             onyxSessionCreateChatSessionTotal.Load(),
		CreateChatSessionSuccessTotal:      onyxSessionCreateChatSessionSuccessTotal.Load(),
		StreamingParentAdvanceTotal:        onyxSessionStreamingParentAdvanceTotal.Load(),
		NonstreamParentRefreshTotal:        onyxSessionNonstreamParentRefreshTotal.Load(),
		NonstreamParentRefreshSuccessTotal: onyxSessionNonstreamParentRefreshSuccessTotal.Load(),
		InvalidSessionRetryTotal:           onyxSessionInvalidSessionRetryTotal.Load(),
		InvalidSessionRetrySuccessTotal:    onyxSessionInvalidSessionRetrySuccessTotal.Load(),
		SharedStoreDegradedReadTotal:       onyxSessionSharedStoreDegradedReadTotal.Load(),
		SharedStoreDegradedWriteTotal:      onyxSessionSharedStoreDegradedWriteTotal.Load(),
	}
	s.SharedStoreHitRate = onyxSessionSafeRate(s.SharedStoreHitTotal, s.SharedStoreHitTotal+s.SharedStoreMissTotal)
	s.InvalidSessionRetrySuccessRate = onyxSessionSafeRate(s.InvalidSessionRetrySuccessTotal, s.InvalidSessionRetryTotal)
	return s
}

func onyxSessionSafeRate(numerator, denominator int64) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func resetOnyxSessionMetricsForTest() {
	onyxSessionLocalHotHitTotal.Store(0)
	onyxSessionSharedStoreHitTotal.Store(0)
	onyxSessionSharedStoreMissTotal.Store(0)
	onyxSessionStatelessBypassTotal.Store(0)
	onyxSessionSharedStoreGetErrorTotal.Store(0)
	onyxSessionSharedStoreSetErrorTotal.Store(0)
	onyxSessionSharedStoreDeleteErrorTotal.Store(0)
	onyxSessionCreateChatSessionTotal.Store(0)
	onyxSessionCreateChatSessionSuccessTotal.Store(0)
	onyxSessionStreamingParentAdvanceTotal.Store(0)
	onyxSessionNonstreamParentRefreshTotal.Store(0)
	onyxSessionNonstreamParentRefreshSuccessTotal.Store(0)
	onyxSessionInvalidSessionRetryTotal.Store(0)
	onyxSessionInvalidSessionRetrySuccessTotal.Store(0)
	onyxSessionSharedStoreDegradedReadTotal.Store(0)
	onyxSessionSharedStoreDegradedWriteTotal.Store(0)
}
