.PHONY: build
build:
	go build -o /root/go/services/log-service/log-service main.go

.PHONY: scp-config
scp-config:
	scp ./config.yaml root@206.81.22.60:/root/go/services/log-service/config.yaml

