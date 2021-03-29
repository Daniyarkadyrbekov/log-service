.PHONY: build
build:
	env GOOS=linux GOARCH=amd64 go build -v main.go

.PHONY: deploy
deploy:
	scp ./bin/log-service root@206.81.22.60:/root/umeford/log-service
