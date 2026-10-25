package redisqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestPayloadLifecycle 验证空消息和任意二进制在全部状态转换中保持原始字节。
func TestPayloadLifecycle(t *testing.T) {
	allBytes := make([]byte, 256)
	for i := range allBytes {
		allBytes[i] = byte(i)
	}
	cases := []struct {
		name string
		data []byte
	}{
		{name: "nil"},
		{name: "empty", data: []byte{}},
		{name: "text", data: []byte("hello")},
		{name: "unicode", data: []byte("中文消息🌏")},
		{name: "binary", data: allBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, cleanup := newTestQueue(t, Config{VisibilityTimeout: time.Second, MaxAttempts: 3})
			defer cleanup()
			ctx := context.Background()
			now := time.Unix(1700000000, 0)
			q.now = func() time.Time { return now }
			created, err := q.Enqueue(ctx, EnqueueRequest{ID: "payload", Payload: tc.data})
			if err != nil {
				t.Fatal(err)
			}
			assertPayload(t, created.Payload, tc.data)
			stored, err := q.client.HGet(ctx, q.keys.data, created.ID).Bytes()
			if err != nil || !bytes.Equal(stored, tc.data) {
				t.Fatalf("Redis 未保存原始字节：got=%x want=%x err=%v", stored, tc.data, err)
			}
			inspected, err := q.InspectTask(ctx, created.ID, true)
			if err != nil || !inspected.HasPayload || inspected.Status != TaskStatusReady {
				t.Fatalf("空消息被误判为不存在：state=%+v err=%v", inspected, err)
			}
			assertPayload(t, inspected.Payload, tc.data)
			hidden, err := q.InspectTask(ctx, created.ID, false)
			if err != nil || !hidden.HasPayload || hidden.Payload != nil {
				t.Fatalf("隐藏正文的结果错误：state=%+v err=%v", hidden, err)
			}
			task := reserveForTest(t, q)
			assertPayload(t, task.Payload, tc.data)
			if result, err := q.Nack(ctx, task.ID, task.Receipt, "retry", true); err != nil || result.Action != "requeued" {
				t.Fatalf("重试失败：result=%+v err=%v", result, err)
			}
			task = reserveForTest(t, q)
			assertPayload(t, task.Payload, tc.data)
			if result, err := q.Nack(ctx, task.ID, task.Receipt, "dead", false); err != nil || result.Action != "dead" {
				t.Fatalf("死信转换失败：result=%+v err=%v", result, err)
			}
			dead, err := q.DeadLetters(ctx, 1)
			if err != nil || len(dead) != 1 {
				t.Fatalf("死信读取失败：tasks=%+v err=%v", dead, err)
			}
			assertPayload(t, dead[0].Payload, tc.data)
			if ok, err := q.RetryDead(ctx, task.ID, nil, true); err != nil || !ok {
				t.Fatalf("死信重投失败：ok=%v err=%v", ok, err)
			}
			task = reserveForTest(t, q)
			assertPayload(t, task.Payload, tc.data)
			now = now.Add(2 * time.Second)
			reclaimed, err := q.ReclaimExpired(ctx, 1)
			if err != nil || len(reclaimed) != 1 || reclaimed[0].Action != "requeued" {
				t.Fatalf("超时回收失败：result=%+v err=%v", reclaimed, err)
			}
			task = reserveForTest(t, q)
			assertPayload(t, task.Payload, tc.data)
			if ok, err := q.Ack(ctx, task.ID, task.Receipt); err != nil || !ok {
				t.Fatalf("确认失败：ok=%v err=%v", ok, err)
			}
			missing, err := q.InspectTask(ctx, task.ID, true)
			if err != nil || missing.HasPayload || missing.Payload != nil {
				t.Fatalf("已完成任务仍有正文：state=%+v err=%v", missing, err)
			}
		})
	}
}

func assertPayload(t *testing.T, got, want []byte) {
	t.Helper()
	if got == nil || !bytes.Equal(got, want) {
		t.Fatalf("消息正文错误：got=%x nil=%v want=%x", got, got == nil, want)
	}
}

// TestPayloadBuffersAreIndependent 验证输入复用和返回值修改不会相互影响或改写 Redis。
func TestPayloadBuffersAreIndependent(t *testing.T) {
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()
	ctx := context.Background()
	want := []byte{0, 255, 'a'}
	input := bytes.Clone(want)
	created, err := q.Enqueue(ctx, EnqueueRequest{ID: "buffers", Payload: input})
	if err != nil {
		t.Fatal(err)
	}
	input[0] = 42
	assertPayload(t, created.Payload, want)
	created.Payload[1] = 17
	if input[1] != want[1] {
		t.Fatal("入队返回值与调用方输入共享可写内存")
	}
	first, err := q.InspectTask(ctx, created.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	assertPayload(t, first.Payload, want)
	first.Payload[0] = 18
	task := reserveForTest(t, q)
	assertPayload(t, task.Payload, want)
	task.Payload[0] = 19
	if _, err := q.Nack(ctx, task.ID, task.Receipt, "dead", false); err != nil {
		t.Fatal(err)
	}
	dead, err := q.DeadLetters(ctx, 1)
	if err != nil || len(dead) != 1 {
		t.Fatalf("死信读取失败：%v %v", dead, err)
	}
	assertPayload(t, dead[0].Payload, want)
	dead[0].Payload[0] = 20
	second, err := q.InspectTask(ctx, task.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	assertPayload(t, second.Payload, want)
}

func TestPayloadLostReplyRecovery(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{name: "nil"},
		{name: "empty", data: []byte{}},
		{name: "binary", data: []byte{0, 255, 128, 'a'}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.data
			srv := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			})
			client.AddHook(&loseEnqueueReply{})
			q := MustNew(client, Config{Name: "lost-payload"})
			ctx := context.Background()
			want := bytes.Clone(data)
			task, err := q.Enqueue(ctx, EnqueueRequest{ID: "payload", Payload: data})
			if err != nil {
				t.Fatal(err)
			}
			if len(data) > 0 {
				data[0] = 42
			}
			assertPayload(t, task.Payload, want)
			if _, err := q.Enqueue(ctx, EnqueueRequest{ID: task.ID, Payload: want}); !errors.Is(err, ErrDuplicateTask) {
				t.Fatalf("恢复后重复任务未被拒绝：%v", err)
			}
			assertPayload(t, reserveForTest(t, q).Payload, want)
		})
	}
}

func TestPayloadJSONRepresentation(t *testing.T) {
	cases := []struct {
		name    string
		value   any
		want    string
		present bool
	}{
		{name: "task", value: Task{Payload: []byte("hello")}, want: `"aGVsbG8="`, present: true},
		{name: "task_empty", value: Task{Payload: []byte{}}, want: `""`, present: true},
		{name: "task_zero", value: Task{}, want: "null", present: true},
		{name: "inspect", value: TaskInspect{HasPayload: true, Payload: []byte("hello")}, want: `"aGVsbG8="`, present: true},
		{name: "inspect_empty", value: TaskInspect{HasPayload: true, Payload: []byte{}}},
		{name: "inspect_hidden", value: TaskInspect{HasPayload: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := json.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			payload, present := fields["payload"]
			if present != tc.present || string(payload) != tc.want {
				t.Fatalf("JSON 正文错误：got=%s present=%v want=%s present=%v", payload, present, tc.want, tc.present)
			}
		})
	}
}

// TestReservePayloadParsing 验证 Redis 返回字节切片时没有别名，并拒绝损坏的正文类型。
func TestReservePayloadParsing(t *testing.T) {
	input := []byte{0, 255, 128}
	reply := []any{"id", input, int64(1), int64(1), int64(100), "receipt"}
	task, err := taskFromReserve(reply)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = 42
	assertPayload(t, task.Payload, []byte{0, 255, 128})
	for _, bad := range []any{nil, int64(7), true} {
		reply[1] = bad
		if _, err := taskFromReserve(reply); err == nil {
			t.Fatalf("接受了非法正文类型：%T", bad)
		}
	}
}

func TestWorkerReceivesBinaryPayload(t *testing.T) {
	q, cleanup := newTestQueue(t, Config{})
	defer cleanup()
	want := []byte{0, 255, 128, 'a'}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := q.Enqueue(ctx, EnqueueRequest{ID: "worker-payload", Payload: want}); err != nil {
		t.Fatal(err)
	}
	received := make(chan []byte, 1)
	done := make(chan error, 1)
	go func() {
		done <- q.Work(ctx, func(_ context.Context, task *Task) error {
			received <- bytes.Clone(task.Payload)
			return nil
		}, WorkerConfig{})
	}()
	// 即使断言失败，也先等待消费者退出，再清理测试队列。
	defer func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("消费者退出错误：%v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("消费者未按时退出")
		}
	}()
	select {
	case payload := <-received:
		assertPayload(t, payload, want)
	case <-time.After(5 * time.Second):
		t.Fatal("消费者未收到消息")
	}
	waitFor(t, 5*time.Second, func() bool {
		stats, err := q.Stats(ctx)
		return err == nil && stats.TotalPayload == 0 && stats.Working == 0
	})
}
