package redisqueue

import (
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func sliceResult(cmd *redis.Cmd) ([]any, error) {
	result, err := cmd.Result()
	if err != nil {
		return nil, err
	}
	items, ok := result.([]any)
	if !ok {
		return nil, fmt.Errorf("redisqueue: unexpected lua result %T", result)
	}
	return items, nil
}

func stringAt(items []any, idx int) (string, error) {
	if idx >= len(items) {
		return "", fmt.Errorf("redisqueue: missing lua result index %d", idx)
	}
	switch v := items[idx].(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprint(v), nil
	}
}

// bytesAt 读取原始字节并返回独立缓冲区，拒绝字符串和字节切片以外的类型。
func bytesAt(items []any, idx int) ([]byte, error) {
	if idx < 0 || idx >= len(items) {
		return nil, fmt.Errorf("redisqueue: missing lua result index %d", idx)
	}
	switch v := items[idx].(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return append([]byte{}, v...), nil
	default:
		return nil, fmt.Errorf("redisqueue: unexpected payload lua result %T", v)
	}
}

func intAt(items []any, idx int) (int64, error) {
	if idx >= len(items) {
		return 0, fmt.Errorf("redisqueue: missing lua result index %d", idx)
	}
	switch v := items[idx].(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("redisqueue: unexpected int lua result %T", v)
	}
}

func parseStringInt64(v string) int64 {
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}

func taskFromReserve(items []any) (*Task, error) {
	id, err := stringAt(items, 0)
	if err != nil {
		return nil, err
	}
	payload, err := bytesAt(items, 1)
	if err != nil {
		return nil, err
	}
	priority, err := intAt(items, 2)
	if err != nil {
		return nil, err
	}
	attempts, err := intAt(items, 3)
	if err != nil {
		return nil, err
	}
	deadline, err := intAt(items, 4)
	if err != nil {
		return nil, err
	}
	receipt, err := stringAt(items, 5)
	if err != nil {
		return nil, err
	}
	return &Task{
		Receipt:    receipt,
		ID:         id,
		Payload:    payload,
		Priority:   priority,
		Attempts:   attempts,
		DeadlineAt: deadline,
	}, nil
}

func reclaimResults(items []any) ([]ReclaimResult, error) {
	if len(items)%3 != 0 {
		return nil, fmt.Errorf("redisqueue: invalid reclaim result length %d", len(items))
	}
	out := make([]ReclaimResult, 0, len(items)/3)
	for i := 0; i < len(items); i += 3 {
		id, err := stringAt(items, i)
		if err != nil {
			return nil, err
		}
		action, err := stringAt(items, i+1)
		if err != nil {
			return nil, err
		}
		attempts, err := intAt(items, i+2)
		if err != nil {
			return nil, err
		}
		out = append(out, ReclaimResult{ID: id, Action: action, Attempts: attempts})
	}
	return out, nil
}
