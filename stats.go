package redisqueue

import (
	"context"
	"log/slog"
	"time"

	"github.com/xiaojiecode/redisqueue/internal/goroutine"
)

// StartStatsReporter 按固定间隔打印队列统计和服务并发槽位统计。
// interval <= 0 时不启动；serviceNames 为空时默认打印 default 服务槽位。
func (q *Queue) StartStatsReporter(ctx context.Context, interval time.Duration, serviceNames ...string) func() {
	return q.startStatsReporter(ctx, interval, serviceNames...)
}

func (q *Queue) startWorkerStatsReporter(ctx context.Context, cfg WorkerConfig) func() {
	if cfg.StatsLogInterval <= 0 {
		return func() {}
	}
	return q.startStatsReporter(ctx, cfg.StatsLogInterval, cfg.ServiceName)
}

func (q *Queue) startStatsReporter(ctx context.Context, interval time.Duration, serviceNames ...string) func() {
	if interval <= 0 {
		return func() {}
	}
	if len(serviceNames) == 0 {
		serviceNames = []string{defaultServiceName}
	}
	reportCtx, cancel := context.WithCancel(ctx)
	goroutine.GoRoutine(reportCtx, "redisqueue.stats."+q.cfg.Name, func(ctx context.Context) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		q.logStats(ctx, serviceNames)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				q.logStats(ctx, serviceNames)
			}
		}
	})
	return cancel
}

func (q *Queue) logStats(ctx context.Context, serviceNames []string) {
	stats, err := q.Stats(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "[RedisQueue:"+q.cfg.Name+"] stats reporter failed", "err", err)
		return
	}
	slog.InfoContext(ctx,
		"[RedisQueue:"+q.cfg.Name+"] stats",
		"ready", stats.Ready,
		"priorityBucket", stats.PriorityBucket,
		"working", stats.Working,
		"processing", stats.Processing,
		"dead", stats.Dead,
		"deadIndex", stats.DeadIndex,
		"totalPayload", stats.TotalPayload,
	)
	for _, serviceName := range serviceNames {
		conc, err := q.ConcurrencyStats(ctx, serviceName)
		if err != nil {
			slog.ErrorContext(ctx, "[RedisQueue:"+q.cfg.Name+"] concurrency stats reporter failed", "service", normalizeServiceName(serviceName), "err", err)
			continue
		}
		slog.InfoContext(ctx,
			"[RedisQueue:"+q.cfg.Name+"] concurrency",
			"service", conc.ServiceName,
			"total", conc.Total,
			"serviceSlots", conc.Service,
		)
	}
}
