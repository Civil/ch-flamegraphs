.PHONY: build

GO ?= go

build:
	$(GO) build --ldflags '-X main.BuildVersion=$(VERSION)'

clean:
	rm -f flamegraph-server
