package goroutine

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestPanicRecoveryAndHandlerIsolation(t *testing.T) {
	done := make(chan string, 1)
	cancel := GoRoutine(context.Background(), "test", func(context.Context) { panic("boom") }, Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Fields: []any{"queue", "test"},
		PanicHandler: func(_ context.Context, name string, value any, stack []byte) {
			if name != "test" || value != "boom" || !strings.Contains(string(stack), "TestPanicRecovery") {
				t.Error("panic 上下文不完整")
			}
			done <- name
			panic("handler panic")
		},
	})
	defer cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("没有收到 panic 回调")
	}
}

func TestContextCancellationAndTimeout(t *testing.T) {
	done := make(chan error, 1)
	cancel := Go("cancel", func(ctx context.Context) { <-ctx.Done(); done <- ctx.Err() })
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Fatal("取消没有传播")
	}
	stop := GoWithTimeout("timeout", 10*time.Millisecond, func(ctx context.Context) { <-ctx.Done(); done <- ctx.Err() })
	defer stop()
	select {
	case err := <-done:
		if err != context.DeadlineExceeded {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Fatal("超时没有传播")
	}
}

func TestDetachedContextKeepsValues(t *testing.T) {
	type key struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "value"))
	cancel()
	done := make(chan struct{}, 1)
	stop := GoDetached(parent, "detached", func(ctx context.Context) {
		if ctx.Err() != nil || ctx.Value(key{}) != "value" {
			t.Error("分离上下文未保留预期语义")
		}
		done <- struct{}{}
	})
	defer stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("协程未执行")
	}
}
