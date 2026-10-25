package redisqueue

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/xiaojiecode/redisqueue/internal/goroutine"
)

// Work 持续领取任务并交给 handler 处理，直到 ctx 结束。
// 空队列时使用可配置自旋退避，避免 Redis 被空轮询打满。
// Concurrency 控制当前进程内最多同时处理多少个任务。
func (q *Queue) Work(ctx context.Context, handler Handler, cfg WorkerConfig) error {
	cfg = cfg.normalize(q.cfg)
	stopStats := q.startWorkerStatsReporter(ctx, cfg)
	defer stopStats()
	idle := cfg.SpinMinInterval
	nextReclaim := q.now().Add(cfg.ReclaimInterval)
	sem := make(chan struct{}, cfg.Concurrency)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for {
		if err := acquireSlot(ctx, sem, errCh); err != nil {
			wg.Wait()
			return err
		}
		slot, ok, reason, err := q.acquireDistributedSlot(ctx, cfg)
		if err != nil {
			releaseSlot(sem)
			return err
		}
		if !ok {
			releaseSlot(sem)
			q.logInfo(ctx, "concurrency limited", "service", cfg.ServiceName, "reason", reason)
			if err := sleepContext(ctx, idle); err != nil {
				wg.Wait()
				return err
			}
			idle = growInterval(idle, cfg.SpinMaxInterval, cfg.SpinFactor)
			continue
		}
		if !q.now().Before(nextReclaim) {
			if _, err := q.ReclaimExpired(ctx, cfg.ReclaimBatch); err != nil {
				q.releaseDistributedSlotWithLog(slot, cfg.ServiceName, "reclaim_failed")
				releaseSlot(sem)
				return err
			}
			nextReclaim = q.now().Add(cfg.ReclaimInterval)
		}
		task, err := q.Reserve(ctx)
		if err != nil {
			q.releaseDistributedSlotWithLog(slot, cfg.ServiceName, "reserve_failed")
			releaseSlot(sem)
			return err
		}
		if task == nil {
			q.releaseDistributedSlotWithLog(slot, cfg.ServiceName, "empty_queue")
			releaseSlot(sem)
			if err := sleepContext(ctx, idle); err != nil {
				wg.Wait()
				return err
			}
			idle = growInterval(idle, cfg.SpinMaxInterval, cfg.SpinFactor)
			continue
		}
		idle = cfg.SpinMinInterval

		currentTask, currentSlot := task, slot
		wg.Add(1)
		goroutine.GoRoutine(ctx, "redisqueue.worker."+cfg.ServiceName, func(workerCtx context.Context) {
			defer wg.Done()
			defer func() {
				if err := q.releaseDistributedSlot(context.Background(), currentSlot); err != nil {
					sendWorkerErr(errCh, err)
				}
			}()
			defer releaseSlot(sem)
			stopRenewal := q.startSlotRenewal(currentSlot, cfg, errCh)
			defer stopRenewal()
			// 业务处理失败必须落到 NACK 状态，确保任务后续重试或进入 DLQ，
			// 不能因为 worker 返回错误而在内存中消失。
			persistCtx := context.WithoutCancel(workerCtx)
			if err := handler(workerCtx, currentTask); err != nil {
				if _, nackErr := q.Nack(persistCtx, currentTask.ID, currentTask.Receipt, err.Error(), true); nackErr != nil {
					sendWorkerErr(errCh, nackErr)
				}
				return
			}
			if _, err := q.Ack(persistCtx, currentTask.ID, currentTask.Receipt); err != nil {
				sendWorkerErr(errCh, err)
			}
		}, goroutine.Options{Fields: []any{"queue", q.cfg.Name, "service", cfg.ServiceName}})
	}
}

func (q *Queue) releaseDistributedSlotWithLog(slot *concurrencySlot, serviceName, stage string) {
	if err := q.releaseDistributedSlot(context.Background(), slot); err != nil {
		slog.Error("redisqueue concurrency slot release failed",
			"queue", q.cfg.Name,
			"service", serviceName,
			"stage", stage,
			"error", err,
		)
	}
}

func acquireSlot(ctx context.Context, sem chan struct{}, errCh <-chan error) error {
	select {
	case err := <-errCh:
		return err
	default:
	}
	select {
	case sem <- struct{}{}:
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func releaseSlot(sem chan struct{}) {
	<-sem
}

func sendWorkerErr(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func growInterval(current, max time.Duration, factor float64) time.Duration {
	next := time.Duration(float64(current) * factor)
	if next > max {
		return max
	}
	if next <= 0 {
		return max
	}
	return next
}
