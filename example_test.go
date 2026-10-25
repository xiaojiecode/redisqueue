package redisqueue_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xiaojiecode/redisqueue"
)

func ExampleQueue() {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()
	q, err := redisqueue.New(client, redisqueue.Config{Name: "example", VisibilityTimeout: time.Minute, MaxAttempts: 3})
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 序列化由调用方负责，可直接传入 json.Marshal 返回的字节。
	payload, err := json.Marshal(struct {
		Message string `json:"message"`
	}{Message: "hello"})
	if err != nil {
		fmt.Println(err)
		return
	}
	if _, err := q.Enqueue(ctx, redisqueue.EnqueueRequest{ID: "task-1", Payload: payload, Priority: 10}); err != nil {
		fmt.Println(err)
		return
	}
	task, err := q.Reserve(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	if task == nil {
		return
	}
	var message struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(task.Payload, &message); err != nil {
		fmt.Println(err)
		return
	}
	// 完成幂等业务处理后，使用本次领取凭证确认任务。
	if _, err := q.Ack(ctx, task.ID, task.Receipt); err != nil {
		fmt.Println(err)
	}
}
