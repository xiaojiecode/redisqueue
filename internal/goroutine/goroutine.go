package goroutine

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"
)

// Options 配置安全协程的 context、超时、日志和 panic 观测行为。
type Options struct {
	// Detach 让协程保留父 context value，但不继承取消与 deadline。
	Detach bool
	// Timeout 为协程设置最长生命周期；小于等于零表示不额外限制。
	Timeout time.Duration
	// Fields 会追加到 panic 日志，内容不得包含密码、token 等敏感值。
	Fields []any
	// Logger 默认使用 slog.Default。
	Logger *slog.Logger
	// PanicHandler 在记录默认 panic 日志后执行，可用于指标或测试断言。
	PanicHandler func(ctx context.Context, name string, recovered any, stack []byte)
}

// GoRoutine 在子 context 中启动 fn，并在协程边界记录和恢复 panic。
func GoRoutine(ctx context.Context, name string, fn func(context.Context), options ...Options) context.CancelFunc {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg := mergeOptions(options)
	if cfg.Detach {
		ctx = context.WithoutCancel(ctx)
	}
	childCtx, cancel := context.WithCancel(ctx)
	if cfg.Timeout > 0 {
		childCtx, cancel = context.WithTimeout(ctx, cfg.Timeout)
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "goroutine"
	}
	go func() {
		defer cancel()
		defer func() {
			if recovered := recover(); recovered != nil {
				handlePanic(childCtx, name, recovered, cfg)
			}
		}()
		fn(childCtx)
	}()
	return cancel
}

// Go 使用 Background context 启动进程级安全协程。
func Go(name string, fn func(context.Context), options ...Options) context.CancelFunc {
	return GoRoutine(context.Background(), name, fn, options...)
}

// GoWithTimeout 使用 Background context 启动带超时的安全协程。
func GoWithTimeout(name string, timeout time.Duration, fn func(context.Context), options ...Options) context.CancelFunc {
	return GoRoutine(context.Background(), name, fn, append(options, Options{Timeout: timeout})...)
}

// GoDetached 启动不跟随父 context 取消、但保留父 value 链的安全协程。
func GoDetached(ctx context.Context, name string, fn func(context.Context), options ...Options) context.CancelFunc {
	return GoRoutine(ctx, name, fn, append(options, Options{Detach: true})...)
}

func mergeOptions(options []Options) Options {
	var cfg Options
	for _, option := range options {
		cfg.Detach = cfg.Detach || option.Detach
		if option.Timeout > 0 {
			cfg.Timeout = option.Timeout
		}
		cfg.Fields = append(cfg.Fields, option.Fields...)
		if option.Logger != nil {
			cfg.Logger = option.Logger
		}
		if option.PanicHandler != nil {
			cfg.PanicHandler = option.PanicHandler
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return cfg
}

func handlePanic(ctx context.Context, name string, recovered any, cfg Options) {
	stack := debug.Stack()
	fields := []any{"name", name, "panic", fmt.Sprint(recovered), "stack", string(stack)}
	fields = append(fields, cfg.Fields...)
	cfg.Logger.ErrorContext(ctx, "goroutine panic", fields...)
	if cfg.PanicHandler == nil {
		return
	}
	defer func() {
		if handlerPanic := recover(); handlerPanic != nil {
			cfg.Logger.ErrorContext(ctx, "goroutine panic handler panic",
				"name", name,
				"panic", fmt.Sprint(handlerPanic),
				"stack", string(debug.Stack()),
			)
		}
	}()
	cfg.PanicHandler(ctx, name, recovered, stack)
}
