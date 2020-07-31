.PHONY: clean
all:
	swag init -g http_server.go
	go build
clean:
	rm -rf store1 store2 store3
	mkdir store1 store2 store3
	rm -rf *.log
	killall -9 worker

