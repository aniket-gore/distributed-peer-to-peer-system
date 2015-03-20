PACKAGES := \
	621_proj/rpcserver \

DEPENDENCIES := github.com/boltdb/bolt 

all: install

install: deps build
	go install testserver/testserver.go
	go install testclient/testclient.go 
build:
	rm -rf $(GOPATH)/src/621_proj
	mkdir -p $(GOPATH)/src/621_proj
	cp -r ./* $(GOPATH)/src/621_proj


server: deps build
	go install testserver.go

format:
	go fmt $(PACKAGES)

deps:
	go get $(DEPENDENCIES)

clean:
	go clean  $(PACKAGES) $(DEPENDENCIES)
	rm -rf $(GOBIN)/testserver
	rm -rf $(GOPATH)/src/621_proj
