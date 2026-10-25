package redisqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/xiaojiecode/redisqueue/internal/goroutine"
)

type concurrencySlot struct {
	token      string
	serviceKey string
	enabled    bool
}

func (q *Queue) acquireDistributedSlot(ctx context.Context, cfg WorkerConfig) (*concurrencySlot, bool, string, error) {
	slot := &concurrencySlot{
		serviceKey: q.keys.serviceConcurrencyKey(cfg.ServiceName),
		enabled:    cfg.ServiceConcurrency > 0 || cfg.TotalConcurrency > 0,
	}
	if !slot.enabled {
		return slot, true, "disabled", nil
	}
	token, err := newToken()
	if err != nil {
		return nil, false, "", err
	}
	slot.token = token
	now := q.now().UnixMilli()
	cmd := scripts.acquire.Run(ctx, q.client,
		[]string{q.keys.concTotal, slot.serviceKey},
		now, now+cfg.ConcurrencyLeaseTTL.Milliseconds(), token, cfg.ServiceConcurrency, cfg.TotalConcurrency,
	)
	reply, err := sliceResult(cmd)
	if err != nil {
		return nil, false, "", err
	}
	ok, err := intAt(reply, 0)
	if err != nil {
		return nil, false, "", err
	}
	reason, err := stringAt(reply, 1)
	if err != nil {
		return nil, false, "", err
	}
	return slot, ok == 1, reason, nil
}

func (q *Queue) releaseDistributedSlot(ctx context.Context, slot *concurrencySlot) error {
	if slot == nil || !slot.enabled || slot.token == "" {
		return nil
	}
	return scripts.release.Run(ctx, q.client, []string{q.keys.concTotal, slot.serviceKey}, slot.token).Err()
}

func (q *Queue) renewDistributedSlot(ctx context.Context, slot *concurrencySlot, cfg WorkerConfig) (bool, error) {
	if slot == nil || !slot.enabled || slot.token == "" {
		return true, nil
	}
	useService := "0"
	if cfg.ServiceConcurrency > 0 {
		useService = "1"
	}
	useTotal := "0"
	if cfg.TotalConcurrency > 0 {
		useTotal = "1"
	}
	deadline := q.now().Add(cfg.ConcurrencyLeaseTTL).UnixMilli()
	cmd := scripts.renew.Run(ctx, q.client, []string{q.keys.concTotal, slot.serviceKey}, slot.token, deadline, useService, useTotal)
	n, err := cmd.Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (q *Queue) startSlotRenewal(slot *concurrencySlot, cfg WorkerConfig, errCh chan<- error) func() {
	if slot == nil || !slot.enabled {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	goroutine.GoRoutine(ctx, "redisqueue.slot_renewal."+cfg.ServiceName, func(ctx context.Context) {
		ticker := time.NewTicker(cfg.ConcurrencyRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ok, err := q.renewDistributedSlot(ctx, slot, cfg)
				if err != nil {
					sendWorkerErr(errCh, err)
					return
				}
				if !ok {
					sendWorkerErr(errCh, fmt.Errorf("redisqueue: concurrency slot expired service=%s", cfg.ServiceName))
					return
				}
			}
		}
	})
	return cancel
}

// ConcurrencyStats 返回队列总并发槽位和指定服务并发槽位的当前占用数。
func (q *Queue) ConcurrencyStats(ctx context.Context, serviceName string) (*ConcurrencyStats, error) {
	serviceName = normalizeServiceName(serviceName)
	now := q.now().UnixMilli()
	serviceKey := q.keys.serviceConcurrencyKey(serviceName)
	pipe := q.client.Pipeline()
	pipe.ZRemRangeByScore(ctx, q.keys.concTotal, "-inf", fmt.Sprint(now))
	pipe.ZRemRangeByScore(ctx, serviceKey, "-inf", fmt.Sprint(now))
	totalCmd := pipe.ZCard(ctx, q.keys.concTotal)
	serviceCmd := pipe.ZCard(ctx, serviceKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return &ConcurrencyStats{
		ServiceName: serviceName,
		Total:       totalCmd.Val(),
		Service:     serviceCmd.Val(),
	}, nil
}

func normalizeServiceName(serviceName string) string {
	serviceName = strings.TrimSpace(serviceName)
	if serviceName == "" {
		return defaultServiceName
	}
	return serviceName
}

func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
