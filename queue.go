package redisqueue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue 是使用 Redis list + zset 实现的可靠优先级队列。
type Queue struct {
	client redis.UniversalClient
	cfg    Config
	keys   keys
	now    clock
}

const enqueueMaxRetries = 3

// New 创建队列实例。每个队列名称会拥有独立的 Redis Key 命名空间。
func New(client redis.UniversalClient, cfg Config) (*Queue, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	normalized, err := cfg.normalize()
	if err != nil {
		return nil, err
	}
	return &Queue{
		client: client,
		cfg:    normalized,
		keys:   newKeys(normalized.baseKey()),
		now:    time.Now,
	}, nil
}

// MustNew 创建队列实例；配置非法时直接 panic，适合初始化阶段使用。
func MustNew(client redis.UniversalClient, cfg Config) *Queue {
	q, err := New(client, cfg)
	if err != nil {
		panic(err)
	}
	return q
}

// Enqueue 原子化保存任务数据，并把任务 ID 追加到对应优先级 list。
// 重复任务 ID 会被拒绝，避免调用方误投导致同一任务重复处理。
// Payload 会被复制，返回的 Task 不共享调用方的输入缓冲区；空输入统一返回非 nil 的空切片。
func (q *Queue) Enqueue(ctx context.Context, req EnqueueRequest) (*Task, error) {
	if req.ID == "" {
		return nil, ErrEmptyTaskID
	}
	// 固定本次投递的字节内容，供 Redis 写入、重试恢复和返回结果共同使用。
	req.Payload = append([]byte{}, req.Payload...)
	now := q.now().UnixMilli()

	// 入队阶段遇到 Redis/Lua 临时错误时，允许同一次调用最多补偿重试 3 次。
	// 如果服务端已经写入成功但客户端回包异常，则尝试按任务 ID 恢复现状，避免把同一次投递误判成重复任务。
	totalAttempts := enqueueMaxRetries + 1
	var (
		reply []any
		err   error
	)
	for attempt := 1; attempt <= totalAttempts; attempt++ {
		cmd := scripts.enqueue.Run(ctx, q.client,
			[]string{q.keys.data, q.keys.priority, q.keys.attempts, q.keys.enqueued, q.keys.updated, q.keys.readyZ},
			req.ID, req.Payload, req.Priority, now, q.keys.readyListKey(req.Priority),
		)
		reply, err = sliceResult(cmd)
		if err == nil {
			break
		}
		if task, ok := q.recoverEnqueueTask(ctx, req, now); ok {
			q.logInfo(ctx, "enqueue recovered after uncertain result", "taskId", req.ID, "priority", req.Priority)
			return task, nil
		}
		if ctx.Err() != nil || attempt == totalAttempts {
			q.logError(ctx, "enqueue failed", "taskId", req.ID, "priority", req.Priority, "attempt", attempt, "maxAttempts", totalAttempts, "err", err)
			return nil, err
		}
		q.logError(ctx, "enqueue attempt failed, retrying", "taskId", req.ID, "priority", req.Priority, "attempt", attempt, "maxAttempts", totalAttempts, "err", err)
	}
	ok, err := intAt(reply, 0)
	if err != nil {
		return nil, err
	}
	if ok == 0 {
		return nil, ErrDuplicateTask
	}
	q.logInfo(ctx, "enqueued", "taskId", req.ID, "priority", req.Priority)
	return &Task{
		ID:         req.ID,
		Payload:    req.Payload,
		Priority:   req.Priority,
		Attempts:   0,
		EnqueuedAt: now,
		UpdatedAt:  now,
	}, nil
}

func (q *Queue) recoverEnqueueTask(ctx context.Context, req EnqueueRequest, now int64) (*Task, bool) {
	task, err := q.loadTask(ctx, req.ID)
	if err != nil || task == nil {
		return nil, false
	}
	if !bytes.Equal(task.Payload, req.Payload) || task.Priority != req.Priority {
		return nil, false
	}
	if task.Attempts != 0 || task.EnqueuedAt != now || task.UpdatedAt != now {
		return nil, false
	}
	return task, true
}

// Reserve 原子化把一个 ready 任务移动到 working 队列，并设置可见性超时。
// 返回 nil task 表示当前没有可消费任务。
func (q *Queue) Reserve(ctx context.Context) (*Task, error) {
	receipt, err := newToken()
	if err != nil {
		return nil, err
	}
	now := q.now().UnixMilli()
	cmd := scripts.reserve.Run(ctx, q.client,
		[]string{
			q.keys.readyZ, q.keys.working, q.keys.processing, q.keys.data,
			q.keys.priority, q.keys.attempts, q.keys.updated, q.keys.receipts,
		},
		now, q.cfg.VisibilityTimeout.Milliseconds(), q.keys.readyList, receipt,
	)
	reply, err := sliceResult(cmd)
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		q.logError(ctx, "reserve failed", "err", err)
		return nil, err
	}
	task, err := taskFromReserve(reply)
	if err != nil {
		return nil, err
	}
	task.UpdatedAt = now
	q.logInfo(ctx, "reserved", "taskId", task.ID, "priority", task.Priority, "attempts", task.Attempts)
	return task, nil
}

// Ack 使用 Reserve 返回的 receipt 确认任务并清理元数据。
// 当前领取凭证不匹配时返回 ErrStaleReceipt；任务已结束时返回 false、nil。
func (q *Queue) Ack(ctx context.Context, id, receipt string) (bool, error) {
	if id == "" {
		return false, ErrEmptyTaskID
	}
	if receipt == "" {
		return false, ErrEmptyReceipt
	}
	cmd := scripts.ack.Run(ctx, q.client,
		[]string{
			q.keys.working, q.keys.processing, q.keys.data, q.keys.priority, q.keys.attempts,
			q.keys.enqueued, q.keys.updated, q.keys.deadReason, q.keys.deadAt, q.keys.receipts,
		},
		id, receipt,
	)
	n, err := cmd.Int64()
	if err != nil {
		q.logError(ctx, "ack failed", "taskId", id, "err", err)
		return false, err
	}
	if n == -1 {
		return false, ErrStaleReceipt
	}
	if n == 1 {
		q.logInfo(ctx, "acked", "taskId", id)
	}
	return n == 1, nil
}

// Nack 标记已领取任务处理失败。requeue=true 时在未超过最大重试次数前重回
// ready 队列；requeue=false 时直接进入死信队列。
// receipt 必须来自当前 Reserve；不匹配时返回 ErrStaleReceipt 且不修改状态。
func (q *Queue) Nack(ctx context.Context, id, receipt string, reason string, requeue bool) (*ReclaimResult, error) {
	if id == "" {
		return nil, ErrEmptyTaskID
	}
	if receipt == "" {
		return nil, ErrEmptyReceipt
	}
	flag := "0"
	if requeue {
		flag = "1"
	}
	now := q.now().UnixMilli()
	cmd := scripts.nack.Run(ctx, q.client,
		[]string{
			q.keys.working, q.keys.processing, q.keys.data, q.keys.priority, q.keys.attempts,
			q.keys.updated, q.keys.readyZ, q.keys.dead, q.keys.deadZ, q.keys.deadReason, q.keys.deadAt, q.keys.receipts,
		},
		id, now, reason, q.cfg.MaxAttempts, flag, q.keys.readyList, receipt,
	)
	reply, err := sliceResult(cmd)
	if err != nil {
		q.logError(ctx, "nack failed", "taskId", id, "err", err)
		return nil, err
	}
	ok, err := intAt(reply, 0)
	if err != nil {
		return nil, err
	}
	if ok == -1 {
		return nil, ErrStaleReceipt
	}
	action, err := stringAt(reply, 1)
	if err != nil {
		return nil, err
	}
	attempts, err := intAt(reply, 2)
	if err != nil {
		return nil, err
	}
	if ok == 0 {
		return &ReclaimResult{ID: id, Action: action, Attempts: attempts}, nil
	}
	q.logInfo(ctx, "nacked", "taskId", id, "action", action, "attempts", attempts)
	return &ReclaimResult{ID: id, Action: action, Attempts: attempts}, nil
}

// ReclaimExpired 回收超时 working 任务，并按尝试次数重回 ready 或进入 DLQ。
// 该方法可由多个 worker 并发调用，状态转换由 Lua 保证原子性。
func (q *Queue) ReclaimExpired(ctx context.Context, limit int64) ([]ReclaimResult, error) {
	if limit <= 0 {
		limit = q.cfg.ReclaimBatchSize
	}
	now := q.now().UnixMilli()
	cmd := scripts.reclaim.Run(ctx, q.client,
		[]string{
			q.keys.working, q.keys.processing, q.keys.data, q.keys.priority, q.keys.attempts,
			q.keys.updated, q.keys.readyZ, q.keys.dead, q.keys.deadZ, q.keys.deadReason, q.keys.deadAt, q.keys.receipts,
		},
		now, limit, q.cfg.MaxAttempts, q.keys.readyList, "visibility timeout",
	)
	reply, err := sliceResult(cmd)
	if err != nil {
		q.logError(ctx, "reclaim failed", "err", err)
		return nil, err
	}
	results, err := reclaimResults(reply)
	if err != nil {
		return nil, err
	}
	for _, item := range results {
		q.logInfo(ctx, "reclaimed", "taskId", item.ID, "action", item.Action, "attempts", item.Attempts)
	}
	return results, nil
}

// RetryDead 将 DLQ 中的任务重新放回 ready 队列。priorityOverride 为 nil 时
// 保留原优先级；resetAttempts=true 时清空历史重试次数。
func (q *Queue) RetryDead(ctx context.Context, id string, priorityOverride *int64, resetAttempts bool) (bool, error) {
	if id == "" {
		return false, ErrEmptyTaskID
	}
	priority := ""
	if priorityOverride != nil {
		priority = strconv.FormatInt(*priorityOverride, 10)
	}
	reset := "0"
	if resetAttempts {
		reset = "1"
	}
	cmd := scripts.retryDead.Run(ctx, q.client,
		[]string{
			q.keys.dead, q.keys.deadZ, q.keys.data, q.keys.priority, q.keys.attempts,
			q.keys.updated, q.keys.deadReason, q.keys.deadAt, q.keys.readyZ,
		},
		id, q.now().UnixMilli(), q.keys.readyList, priority, reset,
	)
	n, err := cmd.Int64()
	if err != nil {
		q.logError(ctx, "retry dead failed", "taskId", id, "err", err)
		return false, err
	}
	if n == 1 {
		q.logInfo(ctx, "retry dead queued", "taskId", id)
	}
	return n == 1, nil
}

// Stats 返回队列长度统计，不读取任务 payload。
func (q *Queue) Stats(ctx context.Context) (*Stats, error) {
	cmd := scripts.stats.Run(ctx, q.client,
		[]string{q.keys.readyZ, q.keys.working, q.keys.processing, q.keys.dead, q.keys.deadZ, q.keys.data},
		q.keys.readyList,
	)
	reply, err := sliceResult(cmd)
	if err != nil {
		return nil, err
	}
	values := make([]int64, 7)
	for i := range values {
		values[i], err = intAt(reply, i)
		if err != nil {
			return nil, err
		}
	}
	return &Stats{
		Ready:          values[0],
		PriorityBucket: values[1],
		Working:        values[2],
		Processing:     values[3],
		Dead:           values[4],
		DeadIndex:      values[5],
		TotalPayload:   values[6],
	}, nil
}

// DeadLetters 按进入 DLQ 的先后顺序返回死信任务。
func (q *Queue) DeadLetters(ctx context.Context, limit int64) ([]Task, error) {
	if limit <= 0 {
		limit = 100
	}
	ids, err := q.client.LRange(ctx, q.keys.dead, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	tasks := make([]Task, 0, len(ids))
	for _, id := range ids {
		task, err := q.loadTask(ctx, id)
		if err != nil {
			return nil, err
		}
		if task != nil {
			tasks = append(tasks, *task)
		}
	}
	return tasks, nil
}

func (q *Queue) loadTask(ctx context.Context, id string) (*Task, error) {
	pipe := q.client.Pipeline()
	payloadCmd := pipe.HGet(ctx, q.keys.data, id)
	priorityCmd := pipe.HGet(ctx, q.keys.priority, id)
	attemptsCmd := pipe.HGet(ctx, q.keys.attempts, id)
	enqueuedCmd := pipe.HGet(ctx, q.keys.enqueued, id)
	updatedCmd := pipe.HGet(ctx, q.keys.updated, id)
	deadAtCmd := pipe.HGet(ctx, q.keys.deadAt, id)
	reasonCmd := pipe.HGet(ctx, q.keys.deadReason, id)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	payload, err := payloadCmd.Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &Task{
		ID:         id,
		Payload:    []byte(payload),
		Priority:   parseStringInt64(priorityCmd.Val()),
		Attempts:   parseStringInt64(attemptsCmd.Val()),
		EnqueuedAt: parseStringInt64(enqueuedCmd.Val()),
		UpdatedAt:  parseStringInt64(updatedCmd.Val()),
		DeadAt:     parseStringInt64(deadAtCmd.Val()),
		Reason:     reasonCmd.Val(),
	}, nil
}

func (q *Queue) logInfo(ctx context.Context, msg string, fields ...any) {
	if !q.cfg.EnableLog {
		return
	}
	slog.InfoContext(ctx, "[RedisQueue:"+q.cfg.Name+"] "+msg, fields...)
}

func (q *Queue) logError(ctx context.Context, msg string, fields ...any) {
	if !q.cfg.EnableLog {
		return
	}
	slog.ErrorContext(ctx, "[RedisQueue:"+q.cfg.Name+"] "+msg, fields...)
}

func formatFields(fields ...any) string {
	if len(fields) == 0 {
		return ""
	}
	var b strings.Builder
	for i := 0; i < len(fields); i += 2 {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(fmt.Sprint(fields[i]))
		if i+1 < len(fields) {
			b.WriteByte('=')
			b.WriteString(fmt.Sprint(fields[i+1]))
		}
	}
	return b.String()
}
