# Reference: https://sohlich.github.io/post/go_makefile/

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GORUN=$(GOCMD) run

default: build clean

build:
	$(GOBUILD) -buildmode=plugin ../mrapps/wc.go
clean:
	rm mr-*