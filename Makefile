PACKAGES := \
	github.com/atulmirajkar/RPC-golang/rpcserver \
	github.com/atulmirajkar/RPC-golang/rpcclient
DEPENDENCIES := github.com/boltdb/bolt 

all: install

install: deps build
	go install testserver/testserver.go
	go install testclient/testclient.go 
build:
	#go get -d github.com/atulmirajkar/RPC-golang
	rm -rf $(GOPATH)/src/github.com/atulmirajkar
	mkdir -p $(GOPATH)/src/github.com/atulmirajkar/RPC-golang
	cp -r ./* $(GOPATH)/src/github.com/atulmirajkar/RPC-golang 

client: build
	go install testclient/testclient.go

server: deps build
	go install testserver/testserver.go

format:
	go fmt $(PACKAGES)

deps:
	go get $(DEPENDENCIES)

clean:
	go clean  $(PACKAGES) $(DEPENDENCIES)
	rm -rf $(GOBIN)/testclient
	rm -rf $(GOBIN)/testserver
	rm -rf $(GOPATH)/src/github.com/atulmirajkar
