package redisqueue

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// testBackend 默认使用内存 Redis；环境变量指定时使用真实服务，连接失败会直接失败。
func testBackend(t *testing.T) redis.UniversalClient {
	t.Helper()
	var client redis.UniversalClient
	if addrs := os.Getenv("REDIS_CLUSTER_ADDRS"); addrs != "" {
		client = redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addrs, ",")})
	} else if addr := os.Getenv("REDIS_TEST_ADDR"); addr != "" {
		client = redis.NewClient(&redis.Options{Addr: addr})
	} else {
		srv := miniredis.RunT(t)
		client = redis.NewClient(&redis.Options{Addr: srv.Addr()})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client
}

// testQueueConfig 为测试分配独立命名空间，避免清理其他应用的数据。
func testQueueConfig(t *testing.T, cfg Config) Config {
	t.Helper()
	token, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Name = "test-" + token
	return cfg
}

func cleanTestQueue(t *testing.T, q *Queue) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clean := func(client redis.Cmdable) error {
		var cursor uint64
		var found []string
		for {
			keys, next, err := client.Scan(ctx, cursor, q.cfg.baseKey()+"*", 100).Result()
			if err != nil {
				return err
			}
			found = append(found, keys...)
			cursor = next
			if cursor == 0 {
				break
			}
		}
		if len(found) > 0 {
			return client.Del(ctx, found...).Err()
		}
		return nil
	}
	var err error
	if cluster, ok := q.client.(*redis.ClusterClient); ok {
		err = cluster.ForEachMaster(ctx, func(_ context.Context, client *redis.Client) error { return clean(client) })
	} else {
		err = clean(q.client)
	}
	if err != nil {
		t.Error(err)
	}
}
