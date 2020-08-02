## build

cd worker; go build


## service start

启动3个store, 启动时里面没有任何raft group
```
goreman start
```


```
sh genworker.sh
```
在3个store里面建立raft group (例子里面gid是200)



## API
check:
http://127.0.0.1:8081/swagger/index.html
http://127.0.0.1:8082/swagger/index.html
http://127.0.0.1:8083/swagger/index.html


## admin 命令行工具

```
cd cmd/admin
go build
```


## benchmark


## TODO

moniter service
