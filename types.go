package redisqueue

import (
	"context"
	"time"
)

const (
	// TaskStatusReady 表示任务仍在 ready 队列中等待消费。
	TaskStatusReady = "ready"
	// TaskStatusWorking 表示任务已被消费者领取，正在 processing 可见性窗口内处理。
	TaskStatusWorking = "working"
	// TaskStatusDead 表示任务已进入死信队列，等待人工或补偿重试。
	TaskStatusDead = "dead"
	// TaskStatusDone 表示任务已完成 ACK，队列侧元数据已清理。
	TaskStatusDone = "done"
	// TaskStatusOrphan 表示存在队列索引但缺少 payload，通常代表异常残留。
	TaskStatusOrphan = "orphan"
)

// EnqueueRequest 描述要写入 ready 队列的任务。
// 调用方负责序列化，且不得在 Enqueue 返回前并发修改 Payload。
type EnqueueRequest struct {
	ID       string // 业务侧唯一任务 ID。
	Payload  []byte // 原始任务字节；nil 和空切片都表示空消息，组件不解析内容。
	Priority int64  // 优先级，数值越大越优先。
}

// Task 是返回给调用方和 worker 的任务信封。
type Task struct {
	Receipt    string `json:"receipt,omitempty"`     // 本次领取的随机凭证，仅领取结果包含；ACK/NACK 必须原样提交。
	ID         string `json:"id"`                    // 业务侧唯一任务 ID。
	Payload    []byte `json:"payload"`               // 原始任务字节；JSON 序列化时编码为 Base64 字符串。
	Priority   int64  `json:"priority"`              // 优先级，数值越大越优先。
	Attempts   int64  `json:"attempts"`              // 已领取尝试次数。
	EnqueuedAt int64  `json:"enqueued_at"`           // 入队时间，毫秒时间戳。
	UpdatedAt  int64  `json:"updated_at"`            // 最近状态更新时间，毫秒时间戳。
	DeadlineAt int64  `json:"deadline_at,omitempty"` // 当前可见性超时截止时间。
	DeadAt     int64  `json:"dead_at,omitempty"`     // 进入 DLQ 的时间。
	Reason     string `json:"reason,omitempty"`      // 最近一次失败或死信原因。
}

// ReclaimResult 表示超时或失败任务最终被如何处理。
type ReclaimResult struct {
	ID       string // 任务 ID。
	Action   string // requeued、dead、missing 或 not_found。
	Attempts int64  // 当前尝试次数。
}

// Stats 是从 Redis 实时读取的队列长度统计。
type Stats struct {
	Ready          int64 `json:"ready"`           // ready 队列任务总数。
	PriorityBucket int64 `json:"priority_bucket"` // 当前非空优先级桶数量。
	Working        int64 `json:"working"`         // working list 长度。
	Processing     int64 `json:"processing"`      // processing zset 长度。
	Dead           int64 `json:"dead"`            // DLQ list 长度。
	DeadIndex      int64 `json:"dead_index"`      // DLQ zset 长度。
	TotalPayload   int64 `json:"total_payload"`   // 仍保留 payload 的任务数量。
}

// ConcurrencyStats 是当前队列的分布式并发槽位统计。
type ConcurrencyStats struct {
	ServiceName string `json:"service_name"` // 查询的服务名称。
	Total       int64  `json:"total"`        // 当前队列已占用的总并发槽位。
	Service     int64  `json:"service"`      // 当前服务已占用的并发槽位。
}

// TaskInspect 是按任务 ID 查询得到的运行状况快照。
type TaskInspect struct {
	ID            string `json:"id"`                       // 任务 ID。
	Status        string `json:"status"`                   // ready、working、dead、done 或 orphan。
	Exists        bool   `json:"exists"`                   // 是否仍有任务元数据或队列索引。
	HasPayload    bool   `json:"has_payload"`              // payload 是否仍保留。
	Payload       []byte `json:"payload,omitempty"`        // 原始任务字节，仅 includePayload=true 且正文存在时返回。
	Priority      int64  `json:"priority"`                 // 优先级。
	Attempts      int64  `json:"attempts"`                 // 已领取尝试次数。
	EnqueuedAt    int64  `json:"enqueued_at"`              // 入队时间，毫秒时间戳。
	UpdatedAt     int64  `json:"updated_at"`               // 最近更新时间，毫秒时间戳。
	DeadlineAt    int64  `json:"deadline_at,omitempty"`    // working 可见性超时截止时间。
	DeadAt        int64  `json:"dead_at,omitempty"`        // 进入 DLQ 的时间。
	Reason        string `json:"reason,omitempty"`         // 死信或失败原因。
	ReadyPosition int64  `json:"ready_position,omitempty"` // ready list 中的位置，0 表示队首；-1 表示不在 ready。
}

// Handler 处理一个已领取任务。返回 nil 会 ACK；返回 error 会 NACK，
// 任务随后会按重试次数重回 ready 队列或进入 DLQ。
type Handler func(ctx context.Context, task *Task) error

type clock func() time.Time
