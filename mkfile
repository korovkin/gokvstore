all: build
	
travis: build
	
build:
	go build

get:
	go get -v ./...
