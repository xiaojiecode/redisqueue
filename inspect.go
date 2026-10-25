package redisqueue

import (
	"context"
)

// InspectTask 按任务 ID 查询运行状况。
// includePayload=false 时只返回是否存在 payload，不返回 payload 内容，避免误用于日志。
// 未请求正文或正文不存在时 Payload 为 nil；存在的空正文返回非 nil 的空切片。
func (q *Queue) InspectTask(ctx context.Context, id string, includePayload bool) (*TaskInspect, error) {
	if id == "" {
		return nil, ErrEmptyTaskID
	}
	include := "0"
	if includePayload {
		include = "1"
	}
	cmd := scripts.inspect.Run(ctx, q.client,
		[]string{
			q.keys.data, q.keys.priority, q.keys.attempts, q.keys.enqueued, q.keys.updated,
			q.keys.working, q.keys.processing, q.keys.deadZ, q.keys.deadReason, q.keys.deadAt,
		},
		id, include, q.keys.readyList,
	)
	reply, err := sliceResult(cmd)
	if err != nil {
		return nil, err
	}
	status, err := stringAt(reply, 0)
	if err != nil {
		return nil, err
	}
	exists, err := intAt(reply, 1)
	if err != nil {
		return nil, err
	}
	hasPayload, err := intAt(reply, 2)
	if err != nil {
		return nil, err
	}
	var payload []byte
	if includePayload && hasPayload == 1 {
		payload, err = bytesAt(reply, 3)
		if err != nil {
			return nil, err
		}
	}
	priority, err := intAt(reply, 4)
	if err != nil {
		return nil, err
	}
	attempts, err := intAt(reply, 5)
	if err != nil {
		return nil, err
	}
	enqueuedAt, err := intAt(reply, 6)
	if err != nil {
		return nil, err
	}
	updatedAt, err := intAt(reply, 7)
	if err != nil {
		return nil, err
	}
	deadlineAt, err := intAt(reply, 8)
	if err != nil {
		return nil, err
	}
	deadAt, err := intAt(reply, 9)
	if err != nil {
		return nil, err
	}
	reason, err := stringAt(reply, 10)
	if err != nil {
		return nil, err
	}
	readyPosition, err := intAt(reply, 11)
	if err != nil {
		return nil, err
	}
	return &TaskInspect{
		ID:            id,
		Status:        status,
		Exists:        exists == 1,
		HasPayload:    hasPayload == 1,
		Payload:       payload,
		Priority:      priority,
		Attempts:      attempts,
		EnqueuedAt:    enqueuedAt,
		UpdatedAt:     updatedAt,
		DeadlineAt:    deadlineAt,
		DeadAt:        deadAt,
		Reason:        reason,
		ReadyPosition: readyPosition,
	}, nil
}
