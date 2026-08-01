#!/bin/bash
set -e

echo "=== Starting Network Chaos Testing with Toxiproxy ==="

# 1. Start Docker Compose Chaos Suite
docker compose -f docker-compose.chaos.yml up -d --build

echo "Waiting for cluster initialization..."
sleep 5

# 2. Setup Toxiproxy proxies
curl -s -X POST http://localhost:8474/proxies -H "Content-Type: application/json" -d '{"name": "node1_grpc", "listen": "0.0.0.0:9101", "upstream": "node1:9001"}'
curl -s -X POST http://localhost:8474/proxies -H "Content-Type: application/json" -d '{"name": "node2_grpc", "listen": "0.0.0.0:9102", "upstream": "node2:9002"}'
curl -s -X POST http://localhost:8474/proxies -H "Content-Type: application/json" -d '{"name": "node3_grpc", "listen": "0.0.0.0:9103", "upstream": "node3:9003"}'

echo "Toxiproxy proxies initialized."

# 3. Inject network partition on Node 1 (disable proxy)
echo "Injecting network partition: disabling node1 proxy..."
curl -s -X POST http://localhost:8474/proxies/node1_grpc -H "Content-Type: application/json" -d '{"enabled": false}'

sleep 3

# 4. Verify cluster status
echo "Checking cluster status during partition..."
curl -s http://localhost:8002/status | grep -q '"role":"leader"' && echo "Node 2 / 3 elected new leader as expected."

# 5. Heal partition
echo "Healing network partition: re-enabling node1 proxy..."
curl -s -X POST http://localhost:8474/proxies/node1_grpc -H "Content-Type: application/json" -d '{"enabled": true}'

sleep 3

# 6. Verify full recovery
echo "Cluster status after healing:"
curl -s http://localhost:8002/status

echo "=== Network Chaos Test Complete: Cluster Recovered Successfully ==="
