# Raft KV Store Cluster Docker Guide

This guide details how to build, run, and test the 3-node distributed KV store cluster using Docker and Docker Compose.

---

## Architecture details
*   **Networking Isolation**: Nodes communicate internally on the private bridge network `raft-net`. The internal gRPC consensus ports (9000) are **never** exposed to the host for security. Only the client-facing HTTP API ports (8080) are mapped to host ports `8001`, `8002`, and `8003` respectively.
*   **Resource Constraints**: Each node is capped at `0.5` CPU cores and `128M` memory.
*   **Non-Root Context**: Nodes execute under a restricted `raft` user context inside the containers.

---

## 1. Start the Cluster
To build the image and bring up the cluster, run:
```bash
docker compose up --build
```
This triggers:
1.  The one-shot `cert-init` container to generate certificates with Docker network SANs (`node1`, `node2`, `node3`) and write them to the host `./certs` directory.
2.  Once `cert-init` exits successfully, the 3 nodes mount their respective read-only cert paths (`./certs/node1`, etc.) and start up.
3.  An election timer fires, and a leader is elected.

---

## 2. Interact with the API
Endpoints are exposed on `8001` (node1), `8002` (node2), and `8003` (node3).

*   **Write a Key (to Node 1):**
    ```bash
    curl -i -X PUT -H "Authorization: Bearer mysecrettoken" -d "docker_value" -L http://localhost:8001/kv/mykey
    ```
    *(Note: The `-L` flag ensures curl follows redirects to the active leader if Node 1 is currently a follower).*

*   **Read a Key (unauthenticated read):**
    ```bash
    curl http://localhost:8001/kv/mykey
    ```

*   **Delete a Key:**
    ```bash
    curl -i -X DELETE -H "Authorization: Bearer mysecrettoken" http://localhost:8001/kv/mykey
    ```

---

## 3. Simulating Failure Modes

### A. Leader Crash & Re-election
1.  Check the logs to find who the current leader is:
    ```bash
    docker compose logs
    ```
2.  Stop the leader container (e.g., `node2`):
    ```bash
    docker compose stop node2
    ```
3.  Check logs to see a follower timeout and elect a new leader.
4.  Write to the new leader (e.g., `node3` at `http://localhost:8003/kv/newkey`).

### B. Recovery & Persistence
1.  Start the stopped node back up:
    ```bash
    docker compose start node2
    ```
2.  The node recovers its WAL/snapshot state and catches up with the active leader.
3.  Verify that `node2` now has the `newkey` written during its downtime:
    ```bash
    curl http://localhost:8002/kv/newkey
    ```

---

## 4. Teardown
To stop the cluster and delete the containers, networks, and persistent data volumes:
```bash
docker compose down -v
```
