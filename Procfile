zero1: ./zero/zero --dir zero/infra1.db --name infra1 --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379 --listen-peer-urls http://127.0.0.1:12380  --advertise-peer-urls http://127.0.0.1:12380 --initial-cluster-token zero-cluster-1 --initial-cluster 'infra1=http://127.0.0.1:12380' --initial-cluster-state new --listen-grpc 127.0.0.1:3401 --listen-http 127.0.0.1:13401
worker1: ./worker/worker -name ./worker/store1 -addr 127.0.0.1:3301 -http_addr 127.0.0.1:8081
worker2: ./worker/worker -name ./worker/store2 -addr 127.0.0.1:3302 -http_addr 127.0.0.1:8082
worker3: ./worker/worker -name ./worker/store3 -addr 127.0.0.1:3303 -http_addr 127.0.0.1:8083
