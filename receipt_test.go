package redisqueue

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func enqueueAndReserve(t *testing.T, q *Queue, id string) *Task {
	t.Helper()
	if _, err := q.Enqueue(context.Background(), EnqueueRequest{ID: id, Payload: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	return reserveForTest(t, q)
}

func reserveForTest(t *testing.T, q *Queue) *Task {
	t.Helper()
	task, err := q.Reserve(context.Background())
	if err != nil || task == nil || task.Receipt == "" {
		t.Fatalf("领取失败：task=%+v err=%v", task, err)
	}
	return task
}

// TestReceiptRejectsPreviousReservation 覆盖超时回收、死信重投与任务 ID 重用。
func TestReceiptRejectsPreviousReservation(t *testing.T) {
	for _, transition := range []string{"reclaim", "nack", "dead_retry", "id_reuse"} {
		t.Run(transition, func(t *testing.T) {
			q, cleanup := newTestQueue(t, Config{VisibilityTimeout: time.Second, MaxAttempts: 3})
			defer cleanup()
			ctx := context.Background()
			now := time.Unix(1700000000, 0)
			q.now = func() time.Time { return now }
			old := enqueueAndReserve(t, q, "receipt-task")
			switch transition {
			case "reclaim":
				now = now.Add(2 * time.Second)
				result, err := q.ReclaimExpired(ctx, 1)
				if err != nil || len(result) != 1 || result[0].Action != "requeued" {
					t.Fatalf("回收失败：%v %v", result, err)
				}
			case "nack":
				if _, err := q.Nack(ctx, old.ID, old.Receipt, "retry", true); err != nil {
					t.Fatal(err)
				}
			case "dead_retry":
				if _, err := q.Nack(ctx, old.ID, old.Receipt, "dead", false); err != nil {
					t.Fatal(err)
				}
				if ok, err := q.RetryDead(ctx, old.ID, nil, true); err != nil || !ok {
					t.Fatalf("重投失败：%v %v", ok, err)
				}
			case "id_reuse":
				if ok, err := q.Ack(ctx, old.ID, old.Receipt); err != nil || !ok {
					t.Fatalf("确认失败：%v %v", ok, err)
				}
				if _, err := q.Enqueue(ctx, EnqueueRequest{ID: old.ID, Payload: []byte("new payload")}); err != nil {
					t.Fatal(err)
				}
			}
			if n, err := q.client.HLen(ctx, q.keys.receipts).Result(); err != nil || n != 0 {
				t.Fatalf("旧凭证未清除：%d %v", n, err)
			}
			current := reserveForTest(t, q)
			if old.Receipt == current.Receipt {
				t.Fatal("再次领取重复使用旧凭证")
			}
			if ok, err := q.Ack(ctx, old.ID, old.Receipt); ok || !errors.Is(err, ErrStaleReceipt) {
				t.Fatalf("旧 ACK 未被拒绝：%v %v", ok, err)
			}
			if _, err := q.Nack(ctx, old.ID, old.Receipt, "stale", false); !errors.Is(err, ErrStaleReceipt) {
				t.Fatalf("旧 NACK 未被拒绝：%v", err)
			}
			state, err := q.InspectTask(ctx, current.ID, true)
			if err != nil || state.Status != TaskStatusWorking || !state.HasPayload || !bytes.Equal(state.Payload, current.Payload) {
				t.Fatalf("旧请求破坏了当前任务：%+v %v", state, err)
			}
			if ok, err := q.Ack(ctx, current.ID, current.Receipt); err != nil || !ok {
				t.Fatalf("当前确认失败：%v %v", ok, err)
			}
			if n, err := q.client.HLen(ctx, q.keys.receipts).Result(); err != nil || n != 0 {
				t.Fatalf("确认后仍有凭证：%d %v", n, err)
			}
		})
	}
}

func TestReceiptRejectsEmptyAndWrongCredentials(t *testing.T) {
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()
	ctx := context.Background()
	task := enqueueAndReserve(t, q, "task")
	for _, receipt := range []string{"", "wrong"} {
		want := ErrStaleReceipt
		if receipt == "" {
			want = ErrEmptyReceipt
		}
		if _, err := q.Ack(ctx, task.ID, receipt); !errors.Is(err, want) {
			t.Fatalf("ACK 错误：%v", err)
		}
		if _, err := q.Nack(ctx, task.ID, receipt, "bad", false); !errors.Is(err, want) {
			t.Fatalf("NACK 错误：%v", err)
		}
	}
	if ok, err := q.Ack(ctx, task.ID, task.Receipt); err != nil || !ok {
		t.Fatalf("确认失败：%v %v", ok, err)
	}
}

func TestReceiptConcurrentOldAndCurrentAcknowledgments(t *testing.T) {
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()
	ctx := context.Background()
	old := enqueueAndReserve(t, q, "task")
	if _, err := q.Nack(ctx, old.ID, old.Receipt, "retry", true); err != nil {
		t.Fatal(err)
	}
	task := reserveForTest(t, q)
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, err := q.Ack(ctx, old.ID, old.Receipt); ok || (err != nil && !errors.Is(err, ErrStaleReceipt)) {
				t.Errorf("旧 ACK：%v %v", ok, err)
			}
		}()
	}
	if ok, err := q.Ack(ctx, task.ID, task.Receipt); err != nil || !ok {
		t.Errorf("当前 ACK：%v %v", ok, err)
	}
	wg.Wait()
}

func TestWorkerPanicLeavesTaskReclaimable(t *testing.T) {
	q, cleanup := newTestQueue(t, Config{VisibilityTimeout: time.Hour})
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "panic", Payload: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- q.Work(ctx, func(context.Context, *Task) error { cancel(); panic("expected test panic") }, WorkerConfig{Concurrency: 1})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker 未退出")
	}
	now := time.Now().Add(2 * time.Hour)
	q.now = func() time.Time { return now }
	result, err := q.ReclaimExpired(context.Background(), 1)
	if err != nil || len(result) != 1 || result[0].Action != "requeued" {
		t.Fatalf("panic 后无法回收：%v %v", result, err)
	}
}
