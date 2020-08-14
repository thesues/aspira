.PHONY: clean, profile
all:
	swag init -g http_server.go
	go build
profile:
	go tool pprof pprof http://127.0.0.1:8081/debug/pprof/profile
clean:
	rm -rf store1 store2 store3
	rm -rf db1 db2 db3
	mkdir store1 store2 store3
	rm -rf *.log
	killall -9 worker

