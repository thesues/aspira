## build

cd worker; go build


## service start

```
worker1: ./worker -id 1 -addr 127.0.0.1:3301
worker2: ./worker -id 2 -addr 127.0.0.1:3302 -join 127.0.0.1:3301
worker3: ./worker -id 3 -addr 127.0.0.1:3303 -join 127.0.0.1:3301
```

## API
check:
http://127.0.0.1:8081/swagger/index.html
http://127.0.0.1:8082/swagger/index.html
http://127.0.0.1:8083/swagger/index.html


## benchmark


## TODO

moniter service
