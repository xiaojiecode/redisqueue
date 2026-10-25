// Package redisqueue 提供基于 Redis 的可靠优先级任务队列。
package redisqueue

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	defaultPrefix            = "redisqueue"
	defaultVisibilityTimeout = 5 * time.Minute
	defaultMaxAttempts       = int64(3)
	defaultReclaimBatchSize  = int64(100)
	defaultSpinMinInterval   = 20 * time.Millisecond
	defaultSpinMaxInterval   = time.Second
	defaultSpinFactor        = 2.0
	defaultReclaimInterval   = 30 * time.Second
	defaultServiceName       = "default"
)

var (
	ErrNilClient      = errors.New("redisqueue: nil redis client")
	ErrEmptyQueueName = errors.New("redisqueue: queue name must not be empty")
	ErrEmptyReceipt   = errors.New("redisqueue: receipt must not be empty")
	ErrStaleReceipt   = errors.New("redisqueue: receipt does not own the current reservation")
	ErrEmptyTaskID    = errors.New("redisqueue: task id must not be empty")
	ErrDuplicateTask  = errors.New("redisqueue: task id already exists")
)

// Config 控制队列 Key、超时和重试等可靠性参数。
//
// 零值字段会使用保守默认值。Name 必填，并会被放入 Redis hash tag，
// 让同一队列的 Key 在 Redis Cluster 中尽量落到同一个 slot。
type Config struct {
	Name              string        // 队列名称，同一业务队列保持唯一。
	Prefix            string        // Redis Key 前缀，默认 redisqueue。
	VisibilityTimeout time.Duration // 任务被领取后的可见性超时。
	MaxAttempts       int64         // 最大领取尝试次数，达到后进入 DLQ。
	ReclaimBatchSize  int64         // 单次超时回收最大任务数。
	EnableLog         bool          // 是否打印关键状态转换日志。
}

func (c Config) normalize() (Config, error) {
	c.Name = strings.TrimSpace(c.Name)
	if c.Name == "" {
		return c, ErrEmptyQueueName
	}
	if c.Prefix == "" {
		c.Prefix = defaultPrefix
	}
	c.Prefix = strings.TrimRight(c.Prefix, ":")
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = defaultVisibilityTimeout
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.ReclaimBatchSize <= 0 {
		c.ReclaimBatchSize = defaultReclaimBatchSize
	}
	return c, nil
}

func (c Config) baseKey() string {
	return fmt.Sprintf("%s:{%s}:", c.Prefix, c.Name)
}

// WorkerConfig 控制 Work 消费循环的自旋退避和超时回收频率。
type WorkerConfig struct {
	ServiceName              string        // 当前消费服务名称，用于服务级分布式并发控制。
	Concurrency              int           // 当前进程内的最大并发消费数。
	ServiceConcurrency       int           // 当前服务在所有实例中的最大并发消费数，0 表示不限制。
	TotalConcurrency         int           // 当前队列在所有服务中的最大并发消费数，0 表示不限制。
	ConcurrencyLeaseTTL      time.Duration // 分布式并发槽位租约时长。
	ConcurrencyRenewInterval time.Duration // 分布式并发槽位续租间隔。
	StatsLogInterval         time.Duration // 周期打印队列统计的间隔，0 表示不打印。
	SpinMinInterval          time.Duration // 空队列时的最小自旋等待间隔。
	SpinMaxInterval          time.Duration // 空队列时的最大自旋等待间隔。
	SpinFactor               float64       // 空队列退避倍率。
	ReclaimInterval          time.Duration // 定时回收超时 working 任务的间隔。
	ReclaimBatch             int64         // 每次回收超时任务的最大数量。
}

func (c WorkerConfig) normalize(queueCfg Config) WorkerConfig {
	c.ServiceName = strings.TrimSpace(c.ServiceName)
	if c.ServiceName == "" {
		c.ServiceName = defaultServiceName
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.ConcurrencyLeaseTTL <= 0 {
		c.ConcurrencyLeaseTTL = queueCfg.VisibilityTimeout
	}
	if c.ConcurrencyRenewInterval <= 0 {
		c.ConcurrencyRenewInterval = c.ConcurrencyLeaseTTL / 3
	}
	if c.ConcurrencyRenewInterval <= 0 {
		c.ConcurrencyRenewInterval = time.Second
	}
	if c.ConcurrencyRenewInterval >= c.ConcurrencyLeaseTTL {
		c.ConcurrencyRenewInterval = c.ConcurrencyLeaseTTL / 2
	}
	if c.ConcurrencyRenewInterval <= 0 {
		c.ConcurrencyRenewInterval = c.ConcurrencyLeaseTTL
	}
	if c.SpinMinInterval <= 0 {
		c.SpinMinInterval = defaultSpinMinInterval
	}
	if c.SpinMaxInterval <= 0 {
		c.SpinMaxInterval = defaultSpinMaxInterval
	}
	if c.SpinMaxInterval < c.SpinMinInterval {
		c.SpinMaxInterval = c.SpinMinInterval
	}
	if c.SpinFactor <= 1 {
		c.SpinFactor = defaultSpinFactor
	}
	if c.ReclaimInterval <= 0 {
		c.ReclaimInterval = defaultReclaimInterval
	}
	if c.ReclaimBatch <= 0 {
		c.ReclaimBatch = queueCfg.ReclaimBatchSize
	}
	return c
}
