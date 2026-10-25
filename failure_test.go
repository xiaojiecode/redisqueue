package redisqueue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// loseEnqueueReply 在 Redis 成功执行后丢弃一次响应，模拟不确定提交结果。
type loseEnqueueReply struct{ lost atomic.Bool }

func (h *loseEnqueueReply) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *loseEnqueueReply) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *loseEnqueueReply) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && (cmd.Name() == "eval" || cmd.Name() == "evalsha") && h.lost.CompareAndSwap(false, true) {
			return errors.New("模拟成功执行后响应丢失")
		}
		return err
	}
}

func TestEnqueueRecoversLostSuccessReply(t *testing.T) {
	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	client.AddHook(&loseEnqueueReply{})
	q := MustNew(client, Config{Name: "lost-reply"})
	ctx := context.Background()
	task, err := q.Enqueue(ctx, EnqueueRequest{ID: "task", Payload: "payload"})
	if err != nil || task == nil {
		t.Fatalf("无法恢复入队结果：%v %v", task, err)
	}
	stats, err := q.Stats(ctx)
	if err != nil || stats.Ready != 1 || stats.TotalPayload != 1 {
		t.Fatalf("重复入队：%+v %v", stats, err)
	}
	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "task", Payload: "payload"}); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("后续重复请求未拒绝：%v", err)
	}
}

func TestClosedClientReturnsErrors(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	q := MustNew(client, Config{Name: "closed-client"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	operations := map[string]func() error{
		"enqueue":     func() error { _, err := q.Enqueue(ctx, EnqueueRequest{ID: "task"}); return err },
		"reserve":     func() error { _, err := q.Reserve(ctx); return err },
		"ack":         func() error { _, err := q.Ack(ctx, "task", "receipt"); return err },
		"nack":        func() error { _, err := q.Nack(ctx, "task", "receipt", "failure", true); return err },
		"reclaim":     func() error { _, err := q.ReclaimExpired(ctx, 1); return err },
		"retry":       func() error { _, err := q.RetryDead(ctx, "task", nil, true); return err },
		"stats":       func() error { _, err := q.Stats(ctx); return err },
		"dead":        func() error { _, err := q.DeadLetters(ctx, 1); return err },
		"inspect":     func() error { _, err := q.InspectTask(ctx, "task", false); return err },
		"concurrency": func() error { _, err := q.ConcurrencyStats(ctx, "service"); return err },
		"work": func() error {
			return q.Work(ctx, func(context.Context, *Task) error { t.Error("连接失败时执行了处理函数"); return nil }, WorkerConfig{})
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if err := operation(); !errors.Is(err, redis.ErrClosed) {
				t.Fatalf("未保留 Redis 错误：%v", err)
			}
		})
	}
}
