.PHONY: clean
all:
	swag init -g http.go
	go build
clean:
	rm *.lusf
	killall -9 worker

