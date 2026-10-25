#!/usr/bin/env bash
set -euo pipefail

# 使用独立 Docker 网络与临时容器验证三主节点，无需开放本机端口。
repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
run_name="redisqueue-test-$$"
redis_image="${REDIS_IMAGE:-redis:7-alpine}"
go_image="${GO_IMAGE:-golang:1.26.5-bookworm}"
cleanup() {
  for n in 1 2 3; do docker rm -f "${run_name}-${n}" >/dev/null 2>&1 || true; done
  docker network rm "$run_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$run_name" >/dev/null
for n in 1 2 3; do
  docker run -d --name "${run_name}-${n}" --network "$run_name" "$redis_image" \
    redis-server --cluster-enabled yes --cluster-node-timeout 5000 --save '' --appendonly no >/dev/null
done
for n in 1 2 3; do
  ready=0
  for attempt in {1..30}; do
    if docker exec "${run_name}-${n}" redis-cli ping 2>/dev/null | grep -q PONG; then ready=1; break; fi
    sleep 1
  done
  test "$ready" = 1
done
docker exec "${run_name}-1" redis-cli --cluster create \
  "${run_name}-1:6379" "${run_name}-2:6379" "${run_name}-3:6379" --cluster-replicas 0 --cluster-yes
ready=0
for attempt in {1..30}; do
  if docker exec "${run_name}-1" redis-cli cluster info | grep -q 'cluster_state:ok'; then ready=1; break; fi
  sleep 1
done
test "$ready" = 1
docker run --rm --network "$run_name" -v "${repo_dir}:/src:ro" -w /src \
  -e "REDIS_CLUSTER_ADDRS=${run_name}-1:6379,${run_name}-2:6379,${run_name}-3:6379" \
  -e GOTOOLCHAIN=local "$go_image" \
  go test -race -shuffle=on -count=3 -timeout=5m ./...
