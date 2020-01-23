all: build
	
travis: build
	
build: get
	go build

get:
	go get -v ./...

test:
	go test -v *_test.go

