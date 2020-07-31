.PHONY: clean
all:
	swag init -g http_server.go
	go build
clean:
	rm *.lusf
	rm *.log
	killall -9 worker

