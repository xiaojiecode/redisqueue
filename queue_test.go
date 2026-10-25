package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestQueue(t *testing.T, cfg Config) (*Queue, func()) {
	t.Helper()
	client := testBackend(t)
	cfg = testQueueConfig(t, cfg)
	if cfg.Name == "" {
		cfg.Name = t.Name()
	}
	if cfg.VisibilityTimeout == 0 {
		cfg.VisibilityTimeout = 10 * time.Millisecond
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 2
	}
	q := MustNew(client, cfg)
	return q, func() { cleanTestQueue(t, q) }
}

func TestEnqueueReservePriorityAndFIFO(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()

	for _, req := range []EnqueueRequest{
		{ID: "low", Payload: "low-payload", Priority: 1},
		{ID: "high-1", Payload: "high-payload-1", Priority: 9},
		{ID: "high-2", Payload: "high-payload-2", Priority: 9},
	} {
		if _, err := q.Enqueue(ctx, req); err != nil {
			t.Fatalf("enqueue %s: %v", req.ID, err)
		}
	}

	for _, want := range []string{"high-1", "high-2", "low"} {
		task, err := q.Reserve(ctx)
		if err != nil {
			t.Fatalf("reserve: %v", err)
		}
		if task == nil || task.ID != want {
			t.Fatalf("reserve id = %#v, want %s", task, want)
		}
		if ok, err := q.Ack(ctx, task.ID, task.Receipt); err != nil || !ok {
			t.Fatalf("ack %s ok=%v err=%v", task.ID, ok, err)
		}
	}

	empty, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("reserve empty: %v", err)
	}
	if empty != nil {
		t.Fatalf("empty reserve = %#v, want nil", empty)
	}
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Ready != 0 || stats.Working != 0 || stats.Processing != 0 || stats.TotalPayload != 0 {
		t.Fatalf("stats after ack = %+v, want empty", stats)
	}
}

func TestEnqueueRejectsDuplicateID(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()

	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "same", Payload: "a", Priority: 1}); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "same", Payload: "b", Priority: 2}); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("enqueue duplicate err = %v, want ErrDuplicateTask", err)
	}
}

func TestEnqueueRetriesTemporaryRedisError(t *testing.T) {
	ctx := context.Background()
	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	client.AddHook(&failFirstProcessHook{remaining: 2})
	q := MustNew(client, Config{
		Name:              t.Name(),
		VisibilityTimeout: 10 * time.Millisecond,
		MaxAttempts:       2,
	})
	defer func() {
		_ = client.Close()
		srv.Close()
	}()

	task, err := q.Enqueue(ctx, EnqueueRequest{ID: "retry-ok", Payload: "payload", Priority: 7})
	if err != nil {
		t.Fatalf("enqueue with retry: %v", err)
	}
	if task == nil || task.ID != "retry-ok" {
		t.Fatalf("enqueue task = %#v, want retry-ok", task)
	}

	reserved, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("reserve after retry: %v", err)
	}
	if reserved == nil || reserved.ID != "retry-ok" {
		t.Fatalf("reserve after retry = %#v, want retry-ok", reserved)
	}
}

func TestEnqueueReturnsErrorAfterRetryLimit(t *testing.T) {
	ctx := context.Background()
	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	client.AddHook(&failFirstProcessHook{remaining: enqueueMaxRetries + 1})
	q := MustNew(client, Config{
		Name:              t.Name(),
		VisibilityTimeout: 10 * time.Millisecond,
		MaxAttempts:       2,
	})
	defer func() {
		_ = client.Close()
		srv.Close()
	}()

	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "retry-fail", Payload: "payload", Priority: 7}); err == nil {
		t.Fatal("enqueue err = nil, want temporary redis error")
	}

	task, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("reserve after failed enqueue: %v", err)
	}
	if task != nil {
		t.Fatalf("reserve after failed enqueue = %#v, want nil", task)
	}
}

func TestPublicMethodsRejectEmptyTaskID(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()

	if _, err := q.Enqueue(ctx, EnqueueRequest{}); !errors.Is(err, ErrEmptyTaskID) {
		t.Fatalf("enqueue empty err = %v, want ErrEmptyTaskID", err)
	}
	if ok, err := q.Ack(ctx, "", "receipt"); !errors.Is(err, ErrEmptyTaskID) || ok {
		t.Fatalf("ack empty ok=%v err=%v, want ErrEmptyTaskID", ok, err)
	}
	if result, err := q.Nack(ctx, "", "receipt", "bad", true); !errors.Is(err, ErrEmptyTaskID) || result != nil {
		t.Fatalf("nack empty result=%+v err=%v, want ErrEmptyTaskID", result, err)
	}
	if ok, err := q.RetryDead(ctx, "", nil, false); !errors.Is(err, ErrEmptyTaskID) || ok {
		t.Fatalf("retry empty ok=%v err=%v, want ErrEmptyTaskID", ok, err)
	}
	if inspected, err := q.InspectTask(ctx, "", false); !errors.Is(err, ErrEmptyTaskID) || inspected != nil {
		t.Fatalf("inspect empty inspected=%+v err=%v, want ErrEmptyTaskID", inspected, err)
	}
}

func TestMissingTaskOperationsAreNoops(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()

	if ok, err := q.Ack(ctx, "missing", "receipt"); err != nil || ok {
		t.Fatalf("ack missing ok=%v err=%v, want false nil", ok, err)
	}
	result, err := q.Nack(ctx, "missing", "receipt", "no such task", true)
	if err != nil {
		t.Fatalf("nack missing: %v", err)
	}
	if result.Action != "not_found" || result.Attempts != 0 {
		t.Fatalf("nack missing result = %+v, want not_found", result)
	}
	if ok, err := q.RetryDead(ctx, "missing", nil, true); err != nil || ok {
		t.Fatalf("retry missing ok=%v err=%v, want false nil", ok, err)
	}
	inspected, err := q.InspectTask(ctx, "missing", false)
	if err != nil {
		t.Fatalf("inspect missing: %v", err)
	}
	if inspected.Status != TaskStatusDone || inspected.Exists || inspected.HasPayload {
		t.Fatalf("inspect missing = %+v, want done non-existent", inspected)
	}
}

func TestNackRequeuesThenMovesToDLQ(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{MaxAttempts: 2})
	defer cleanup()

	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "task", Payload: "payload", Priority: 3}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	first, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("reserve first: %v", err)
	}
	if first.Attempts != 1 {
		t.Fatalf("first attempts = %d, want 1", first.Attempts)
	}
	result, err := q.Nack(ctx, first.ID, first.Receipt, "temporary error", true)
	if err != nil {
		t.Fatalf("nack first: %v", err)
	}
	if result.Action != "requeued" {
		t.Fatalf("first nack action = %s, want requeued", result.Action)
	}

	second, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("reserve second: %v", err)
	}
	if second.Attempts != 2 {
		t.Fatalf("second attempts = %d, want 2", second.Attempts)
	}
	result, err = q.Nack(ctx, second.ID, second.Receipt, "permanent error", true)
	if err != nil {
		t.Fatalf("nack second: %v", err)
	}
	if result.Action != "dead" {
		t.Fatalf("second nack action = %s, want dead", result.Action)
	}

	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Dead != 1 || stats.Ready != 0 || stats.Working != 0 {
		t.Fatalf("stats after dlq = %+v", stats)
	}
	dead, err := q.DeadLetters(ctx, 10)
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != "task" || dead[0].Reason != "permanent error" {
		t.Fatalf("dead letters = %+v", dead)
	}
}

func TestNackWithoutRequeueMovesToDLQImmediately(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{MaxAttempts: 10})
	defer cleanup()

	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "no-requeue", Payload: "payload", Priority: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	task, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	result, err := q.Nack(ctx, task.ID, task.Receipt, "manual dead", false)
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if result.Action != "dead" || result.Attempts != 1 {
		t.Fatalf("nack result = %+v, want dead attempts 1", result)
	}
	inspected, err := q.InspectTask(ctx, task.ID, false)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if inspected.Status != TaskStatusDead || inspected.Reason != "manual dead" {
		t.Fatalf("inspect = %+v, want dead manual reason", inspected)
	}
}

func TestConcurrentReserveGetsEachTaskOnce(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()

	const total = 50
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("task-%02d", i)
		if _, err := q.Enqueue(ctx, EnqueueRequest{ID: id, Payload: "payload", Priority: int64(i % 5)}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	// 并发领取是可靠队列最容易出竞态的路径：这里验证 Lua Reserve
	// 在高并发下仍然保证每个任务 ID 只会进入一个 worker。
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[string]int, total)
	errCh := make(chan error, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, err := q.Reserve(ctx)
			if err != nil {
				errCh <- err
				return
			}
			if task == nil {
				errCh <- errors.New("reserve returned nil before queue drained")
				return
			}
			mu.Lock()
			seen[task.ID]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != total {
		t.Fatalf("seen len = %d, want %d", len(seen), total)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("task %s reserved %d times, want once", id, count)
		}
	}
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Ready != 0 || stats.Working != total || stats.Processing != total {
		t.Fatalf("stats after concurrent reserve = %+v", stats)
	}
}

func TestConcurrentAckOnlyOneSucceeds(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()

	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "ack-race", Payload: "payload", Priority: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	task, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 同一任务可能因为调用方重试或超时回调被多次 ACK，Lua Ack 必须
	// 保证只有第一次真正清理 working/processing 并返回成功。
	var success int64
	var wg sync.WaitGroup
	errCh := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := q.Ack(ctx, task.ID, task.Receipt)
			if err != nil {
				errCh <- err
				return
			}
			if ok {
				atomic.AddInt64(&success, 1)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt64(&success); got != 1 {
		t.Fatalf("ack success = %d, want 1", got)
	}
}

func TestReclaimExpiredRequeuesThenDLQ(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{VisibilityTimeout: 10 * time.Millisecond, MaxAttempts: 2})
	defer cleanup()

	now := time.UnixMilli(1000)
	q.now = func() time.Time { return now }
	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "exp", Payload: "payload", Priority: 4}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := q.Reserve(ctx); err != nil {
		t.Fatalf("reserve first: %v", err)
	}
	now = now.Add(11 * time.Millisecond)
	results, err := q.ReclaimExpired(ctx, 10)
	if err != nil {
		t.Fatalf("reclaim first: %v", err)
	}
	if len(results) != 1 || results[0].Action != "requeued" || results[0].Attempts != 1 {
		t.Fatalf("first reclaim = %+v", results)
	}

	if _, err := q.Reserve(ctx); err != nil {
		t.Fatalf("reserve second: %v", err)
	}
	now = now.Add(11 * time.Millisecond)
	results, err = q.ReclaimExpired(ctx, 10)
	if err != nil {
		t.Fatalf("reclaim second: %v", err)
	}
	if len(results) != 1 || results[0].Action != "dead" || results[0].Attempts != 2 {
		t.Fatalf("second reclaim = %+v", results)
	}
}

func TestReclaimExpiredHonorsLimit(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{VisibilityTimeout: 10 * time.Millisecond, MaxAttempts: 3})
	defer cleanup()

	now := time.UnixMilli(2000)
	q.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("limit-%d", i)
		if _, err := q.Enqueue(ctx, EnqueueRequest{ID: id, Payload: "payload", Priority: 1}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
		if _, err := q.Reserve(ctx); err != nil {
			t.Fatalf("reserve %s: %v", id, err)
		}
	}
	now = now.Add(11 * time.Millisecond)
	results, err := q.ReclaimExpired(ctx, 2)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("reclaimed len = %d, want 2", len(results))
	}
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Ready != 2 || stats.Working != 1 || stats.Processing != 1 {
		t.Fatalf("stats after limited reclaim = %+v", stats)
	}
}

func TestRetryDeadResetsAttemptsAndPriority(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{MaxAttempts: 1})
	defer cleanup()

	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "dead", Payload: "payload", Priority: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	task, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := q.Nack(ctx, task.ID, task.Receipt, "fail", true); err != nil {
		t.Fatalf("nack: %v", err)
	}
	priority := int64(8)
	ok, err := q.RetryDead(ctx, "dead", &priority, true)
	if err != nil {
		t.Fatalf("retry dead: %v", err)
	}
	if !ok {
		t.Fatal("retry dead ok=false, want true")
	}
	task, err = q.Reserve(ctx)
	if err != nil {
		t.Fatalf("reserve retry: %v", err)
	}
	if task.Priority != 8 || task.Attempts != 1 {
		t.Fatalf("retried task = %+v, want priority 8 attempts 1", task)
	}
}

func TestWorkAcksSuccessfulTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()

	if _, err := q.Enqueue(context.Background(), EnqueueRequest{ID: "ok", Payload: "payload", Priority: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	processed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- q.Work(ctx, func(ctx context.Context, task *Task) error {
			if task.ID != "ok" {
				t.Errorf("task id = %s, want ok", task.ID)
			}
			close(processed)
			return nil
		}, WorkerConfig{SpinMinInterval: time.Millisecond, SpinMaxInterval: time.Millisecond})
	}()

	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("worker did not process task")
	}
	waitFor(t, time.Second, func() bool {
		stats, err := q.Stats(context.Background())
		return err == nil && stats.TotalPayload == 0
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("work err = %v, want context.Canceled", err)
	}
}

func TestWorkNacksFailedTaskToDLQ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q, cleanup := newTestQueue(t, Config{MaxAttempts: 1})
	defer cleanup()

	if _, err := q.Enqueue(context.Background(), EnqueueRequest{ID: "fail", Payload: "payload", Priority: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	processed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- q.Work(ctx, func(ctx context.Context, task *Task) error {
			close(processed)
			return errors.New("handler failed")
		}, WorkerConfig{SpinMinInterval: time.Millisecond, SpinMaxInterval: time.Millisecond})
	}()

	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("worker did not process failed task")
	}
	waitFor(t, time.Second, func() bool {
		stats, err := q.Stats(context.Background())
		return err == nil && stats.Dead == 1
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("work err = %v, want context.Canceled", err)
	}
}

func TestWorkRespectsConcurrencyLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()

	for i := 0; i < 4; i++ {
		id := "task-" + string(rune('a'+i))
		if _, err := q.Enqueue(context.Background(), EnqueueRequest{ID: id, Payload: "payload", Priority: 1}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	var current int64
	var maxSeen int64
	var processed int64
	done := make(chan error, 1)
	go func() {
		done <- q.Work(ctx, func(ctx context.Context, task *Task) error {
			now := atomic.AddInt64(&current, 1)
			for {
				max := atomic.LoadInt64(&maxSeen)
				if now <= max || atomic.CompareAndSwapInt64(&maxSeen, max, now) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt64(&current, -1)
			atomic.AddInt64(&processed, 1)
			return nil
		}, WorkerConfig{Concurrency: 2, SpinMinInterval: time.Millisecond, SpinMaxInterval: time.Millisecond})
	}()

	waitFor(t, time.Second, func() bool {
		return atomic.LoadInt64(&processed) == 4
	})
	if got := atomic.LoadInt64(&maxSeen); got > 2 {
		t.Fatalf("max concurrency = %d, want <= 2", got)
	}
	waitFor(t, time.Second, func() bool {
		stats, err := q.Stats(context.Background())
		return err == nil && stats.TotalPayload == 0
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("work err = %v, want context.Canceled", err)
	}
}

func TestWorkRespectsDistributedTotalConcurrencyLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q1, q2, cleanup := newSharedQueues(t)
	defer cleanup()

	for i := 0; i < 6; i++ {
		id := "total-task-" + string(rune('a'+i))
		if _, err := q1.Enqueue(context.Background(), EnqueueRequest{ID: id, Payload: "payload", Priority: 1}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	var current int64
	var maxSeen int64
	var processed int64
	handler := func(ctx context.Context, task *Task) error {
		now := atomic.AddInt64(&current, 1)
		recordMax(&maxSeen, now)
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt64(&current, -1)
		atomic.AddInt64(&processed, 1)
		return nil
	}
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	cfgA := WorkerConfig{
		ServiceName:              "svc-a",
		Concurrency:              4,
		TotalConcurrency:         2,
		ConcurrencyLeaseTTL:      100 * time.Millisecond,
		ConcurrencyRenewInterval: 20 * time.Millisecond,
		SpinMinInterval:          time.Millisecond,
		SpinMaxInterval:          time.Millisecond,
	}
	cfgB := cfgA
	cfgB.ServiceName = "svc-b"
	go func() { done1 <- q1.Work(ctx, handler, cfgA) }()
	go func() { done2 <- q2.Work(ctx, handler, cfgB) }()

	waitFor(t, time.Second, func() bool {
		return atomic.LoadInt64(&processed) == 6
	})
	if got := atomic.LoadInt64(&maxSeen); got > 2 {
		t.Fatalf("max total concurrency = %d, want <= 2", got)
	}
	cancel()
	if err := <-done1; !errors.Is(err, context.Canceled) {
		t.Fatalf("work1 err = %v, want context.Canceled", err)
	}
	if err := <-done2; !errors.Is(err, context.Canceled) {
		t.Fatalf("work2 err = %v, want context.Canceled", err)
	}
}

func TestWorkRespectsDistributedServiceConcurrencyLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q1, q2, cleanup := newSharedQueues(t)
	defer cleanup()

	for i := 0; i < 4; i++ {
		id := "service-task-" + string(rune('a'+i))
		if _, err := q1.Enqueue(context.Background(), EnqueueRequest{ID: id, Payload: "payload", Priority: 1}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	var current int64
	var maxSeen int64
	var processed int64
	handler := func(ctx context.Context, task *Task) error {
		now := atomic.AddInt64(&current, 1)
		recordMax(&maxSeen, now)
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt64(&current, -1)
		atomic.AddInt64(&processed, 1)
		return nil
	}
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	cfg := WorkerConfig{
		ServiceName:              "same-service",
		Concurrency:              4,
		ServiceConcurrency:       1,
		TotalConcurrency:         10,
		ConcurrencyLeaseTTL:      100 * time.Millisecond,
		ConcurrencyRenewInterval: 20 * time.Millisecond,
		SpinMinInterval:          time.Millisecond,
		SpinMaxInterval:          time.Millisecond,
	}
	go func() { done1 <- q1.Work(ctx, handler, cfg) }()
	go func() { done2 <- q2.Work(ctx, handler, cfg) }()

	waitFor(t, time.Second, func() bool {
		return atomic.LoadInt64(&processed) == 4
	})
	if got := atomic.LoadInt64(&maxSeen); got > 1 {
		t.Fatalf("max service concurrency = %d, want <= 1", got)
	}
	cancel()
	if err := <-done1; !errors.Is(err, context.Canceled) {
		t.Fatalf("work1 err = %v, want context.Canceled", err)
	}
	if err := <-done2; !errors.Is(err, context.Canceled) {
		t.Fatalf("work2 err = %v, want context.Canceled", err)
	}
}

func TestDistributedConcurrencyStatsAndLimits(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()
	cfg := WorkerConfig{
		ServiceName:         "svc-a",
		ServiceConcurrency:  2,
		TotalConcurrency:    3,
		ConcurrencyLeaseTTL: time.Second,
	}.normalize(q.cfg)

	slot1, ok, reason, err := q.acquireDistributedSlot(ctx, cfg)
	if err != nil || !ok {
		t.Fatalf("acquire slot1 ok=%v reason=%s err=%v", ok, reason, err)
	}
	defer q.releaseDistributedSlot(ctx, slot1)
	slot2, ok, reason, err := q.acquireDistributedSlot(ctx, cfg)
	if err != nil || !ok {
		t.Fatalf("acquire slot2 ok=%v reason=%s err=%v", ok, reason, err)
	}
	defer q.releaseDistributedSlot(ctx, slot2)

	stats, err := q.ConcurrencyStats(ctx, "svc-a")
	if err != nil {
		t.Fatalf("concurrency stats: %v", err)
	}
	if stats.Total != 2 || stats.Service != 2 {
		t.Fatalf("stats = %+v, want total=2 service=2", stats)
	}
	_, ok, reason, err = q.acquireDistributedSlot(ctx, cfg)
	if err != nil {
		t.Fatalf("acquire over service: %v", err)
	}
	if ok || reason != "service" {
		t.Fatalf("over service ok=%v reason=%s, want service limit", ok, reason)
	}

	cfgB := cfg
	cfgB.ServiceName = "svc-b"
	slot3, ok, reason, err := q.acquireDistributedSlot(ctx, cfgB)
	if err != nil || !ok {
		t.Fatalf("acquire slot3 ok=%v reason=%s err=%v", ok, reason, err)
	}
	defer q.releaseDistributedSlot(ctx, slot3)

	cfgC := cfg
	cfgC.ServiceName = "svc-c"
	_, ok, reason, err = q.acquireDistributedSlot(ctx, cfgC)
	if err != nil {
		t.Fatalf("acquire over total: %v", err)
	}
	if ok || reason != "total" {
		t.Fatalf("over total ok=%v reason=%s, want total limit", ok, reason)
	}
}

func TestDistributedConcurrencyLeaseExpiryFreesSlots(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()
	now := time.UnixMilli(3000)
	q.now = func() time.Time { return now }
	// 服务异常退出时无法主动释放并发槽位，因此必须依赖租约过期恢复容量。
	cfg := WorkerConfig{
		ServiceName:         "lease-service",
		ServiceConcurrency:  1,
		TotalConcurrency:    1,
		ConcurrencyLeaseTTL: 10 * time.Millisecond,
	}.normalize(q.cfg)

	slot, ok, reason, err := q.acquireDistributedSlot(ctx, cfg)
	if err != nil || !ok {
		t.Fatalf("acquire first ok=%v reason=%s err=%v", ok, reason, err)
	}
	defer q.releaseDistributedSlot(ctx, slot)
	_, ok, reason, err = q.acquireDistributedSlot(ctx, cfg)
	if err != nil {
		t.Fatalf("acquire before expiry: %v", err)
	}
	if ok || reason != "total" {
		t.Fatalf("before expiry ok=%v reason=%s, want total limit", ok, reason)
	}
	now = now.Add(11 * time.Millisecond)
	slot2, ok, reason, err := q.acquireDistributedSlot(ctx, cfg)
	if err != nil || !ok {
		t.Fatalf("acquire after expiry ok=%v reason=%s err=%v", ok, reason, err)
	}
	defer q.releaseDistributedSlot(ctx, slot2)
}

func TestDistributedConcurrencyRenewKeepsSlots(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()
	now := time.UnixMilli(4000)
	q.now = func() time.Time { return now }
	// 长任务处理期间会定时续租，续租成功后旧槽位不能被其他 worker 抢走。
	cfg := WorkerConfig{
		ServiceName:         "renew-service",
		ServiceConcurrency:  1,
		TotalConcurrency:    1,
		ConcurrencyLeaseTTL: 10 * time.Millisecond,
	}.normalize(q.cfg)

	slot, ok, reason, err := q.acquireDistributedSlot(ctx, cfg)
	if err != nil || !ok {
		t.Fatalf("acquire ok=%v reason=%s err=%v", ok, reason, err)
	}
	defer q.releaseDistributedSlot(ctx, slot)
	now = now.Add(5 * time.Millisecond)
	renewed, err := q.renewDistributedSlot(ctx, slot, cfg)
	if err != nil || !renewed {
		t.Fatalf("renewed=%v err=%v, want true nil", renewed, err)
	}
	now = now.Add(6 * time.Millisecond)
	_, ok, reason, err = q.acquireDistributedSlot(ctx, cfg)
	if err != nil {
		t.Fatalf("acquire after renewed lease: %v", err)
	}
	if ok || reason != "total" {
		t.Fatalf("after renew ok=%v reason=%s, want total limit", ok, reason)
	}
}

func TestStartStatsReporterCanStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()

	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "stats-task", Payload: "payload", Priority: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	stop := q.StartStatsReporter(ctx, time.Millisecond, "svc-a")
	time.Sleep(5 * time.Millisecond)
	stop()
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats after reporter: %v", err)
	}
	if stats.Ready != 1 {
		t.Fatalf("ready = %d, want 1", stats.Ready)
	}
}

func TestInspectTaskLifecycle(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{MaxAttempts: 1})
	defer cleanup()

	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "inspect", Payload: "secret-payload", Priority: 5}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	inspected, err := q.InspectTask(ctx, "inspect", false)
	if err != nil {
		t.Fatalf("inspect ready: %v", err)
	}
	if inspected.Status != TaskStatusReady || !inspected.Exists || !inspected.HasPayload || inspected.Payload != "" || inspected.ReadyPosition != 0 {
		t.Fatalf("ready inspect = %+v", inspected)
	}

	task, err := q.Reserve(ctx)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	inspected, err = q.InspectTask(ctx, task.ID, true)
	if err != nil {
		t.Fatalf("inspect working: %v", err)
	}
	if inspected.Status != TaskStatusWorking || inspected.Payload != "secret-payload" || inspected.Attempts != 1 || inspected.DeadlineAt == 0 {
		t.Fatalf("working inspect = %+v", inspected)
	}

	if _, err := q.Nack(ctx, task.ID, task.Receipt, "inspect failed", true); err != nil {
		t.Fatalf("nack: %v", err)
	}
	inspected, err = q.InspectTask(ctx, task.ID, false)
	if err != nil {
		t.Fatalf("inspect dead: %v", err)
	}
	if inspected.Status != TaskStatusDead || inspected.Reason != "inspect failed" || inspected.DeadAt == 0 {
		t.Fatalf("dead inspect = %+v", inspected)
	}

	if ok, err := q.RetryDead(ctx, task.ID, nil, true); err != nil || !ok {
		t.Fatalf("retry dead ok=%v err=%v", ok, err)
	}
	inspected, err = q.InspectTask(ctx, task.ID, false)
	if err != nil {
		t.Fatalf("inspect retried: %v", err)
	}
	if inspected.Status != TaskStatusReady || inspected.Attempts != 0 {
		t.Fatalf("retried inspect = %+v", inspected)
	}

	task, err = q.Reserve(ctx)
	if err != nil {
		t.Fatalf("reserve retried: %v", err)
	}
	if ok, err := q.Ack(ctx, task.ID, task.Receipt); err != nil || !ok {
		t.Fatalf("ack ok=%v err=%v", ok, err)
	}
	inspected, err = q.InspectTask(ctx, task.ID, false)
	if err != nil {
		t.Fatalf("inspect done: %v", err)
	}
	if inspected.Status != TaskStatusDone || inspected.Exists || inspected.HasPayload {
		t.Fatalf("done inspect = %+v", inspected)
	}
}

func TestInspectTaskReadyPositionAndOrphan(t *testing.T) {
	ctx := context.Background()
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()

	for _, id := range []string{"first", "second"} {
		if _, err := q.Enqueue(ctx, EnqueueRequest{ID: id, Payload: "payload", Priority: 3}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	inspected, err := q.InspectTask(ctx, "second", false)
	if err != nil {
		t.Fatalf("inspect second: %v", err)
	}
	if inspected.Status != TaskStatusReady || inspected.ReadyPosition != 1 {
		t.Fatalf("second inspect = %+v, want ready position 1", inspected)
	}
	// 模拟 Redis 元数据被异常破坏的残留场景，InspectTask 应能识别 orphan，
	// 方便运维侧定位需要人工清理或重建的消息。
	if err := q.client.HDel(ctx, q.keys.data, "second").Err(); err != nil {
		t.Fatalf("remove payload: %v", err)
	}
	inspected, err = q.InspectTask(ctx, "second", false)
	if err != nil {
		t.Fatalf("inspect orphan: %v", err)
	}
	if inspected.Status != TaskStatusOrphan || !inspected.Exists || inspected.HasPayload {
		t.Fatalf("orphan inspect = %+v, want orphan existing index without payload", inspected)
	}
}

func TestGrowInterval(t *testing.T) {
	if got := growInterval(10*time.Millisecond, 100*time.Millisecond, 2); got != 20*time.Millisecond {
		t.Fatalf("grow = %v, want 20ms", got)
	}
	if got := growInterval(80*time.Millisecond, 100*time.Millisecond, 2); got != 100*time.Millisecond {
		t.Fatalf("grow capped = %v, want 100ms", got)
	}
}

func newSharedQueues(t *testing.T) (*Queue, *Queue, func()) {
	t.Helper()
	client := testBackend(t)
	cfg := testQueueConfig(t, Config{VisibilityTimeout: time.Second, MaxAttempts: 2})
	q1 := MustNew(client, cfg)
	var second redis.UniversalClient
	switch client := client.(type) {
	case *redis.Client:
		second = redis.NewClient(client.Options())
	case *redis.ClusterClient:
		second = redis.NewClusterClient(client.Options())
	default:
		t.Fatalf("不支持的测试客户端：%T", client)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Error(err)
		}
	})
	q2 := MustNew(second, cfg)
	return q1, q2, func() { cleanTestQueue(t, q1) }
}

func recordMax(maxSeen *int64, current int64) {
	for {
		max := atomic.LoadInt64(maxSeen)
		if current <= max || atomic.CompareAndSwapInt64(maxSeen, max, current) {
			return
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

type failFirstProcessHook struct {
	remaining int32
}

func (h *failFirstProcessHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *failFirstProcessHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "eval", "evalsha":
			if atomic.AddInt32(&h.remaining, -1) >= 0 {
				return errors.New("temporary enqueue failure")
			}
		}
		return next(ctx, cmd)
	}
}

func (h *failFirstProcessHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
