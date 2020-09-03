zero1: ./zero/zero --dir infra1.db --name infra1 --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379 --listen-peer-urls http://127.0.0.1:12380  --advertise-peer-urls http://127.0.0.1:12380 --initial-cluster-token zero-cluster-1 --initial-cluster 'infra1=http://127.0.0.1:12380' --initial-cluster-state new --listen-grpc 127.0.0.1:3401 --listen-http 0.0.0.0:13401
worker1: ./worker/worker -name /home/ubuntu/x -addr 127.0.0.1:3301 -http_addr 0.0.0.0:8081
worker2: ./worker/worker -name /home/ubuntu/y -addr 127.0.0.1:3302 -http_addr 0.0.0.0:8082
worker3: ./worker/worker -name /home/ubuntu/z -addr 127.0.0.1:3303 -http_addr 0.0.0.0:8083
