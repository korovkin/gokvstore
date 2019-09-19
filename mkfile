all: build
	
travis: build
	
build: get
	go build

get:
	go get -v ./...
