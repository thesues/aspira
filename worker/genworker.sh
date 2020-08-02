gid=200
curl -X POST "http://127.0.0.1:8081/addworker" -H "Content-Type: application/x-www-form-urlencoded" -d "id=1&gid=${gid}"
sleep 2
curl -X POST "http://127.0.0.1:8082/addworker" -H "Content-Type: application/x-www-form-urlencoded" -d "id=2&gid=${gid}&joinCluster=127.0.0.1:3301"
curl -X POST "http://127.0.0.1:8083/addworker" -H "Content-Type: application/x-www-form-urlencoded" -d "id=3&gid=${gid}&joinCluster=127.0.0.1:3301"
