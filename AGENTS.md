# 协作约定

- 使用中文说明、代码注释和 Git 提交信息。
- 修改前阅读 README.md，保留现有行为；接口、数据结构或并发规则的变化必须先讨论确认。
- 验证格式、`go vet ./...`、`go test -race ./...`；Redis 行为还须设置 `REDIS_TEST_ADDR` 进行真实 Redis 验证。
- 测试使用独立键名，不执行 FLUSHDB、FLUSHALL，不接入业务环境。
- 更新公开接口时同步修改 README、示例和回归测试。
