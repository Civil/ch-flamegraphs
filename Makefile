all: carbonserver-collector flamegraph-server
VERSION ?= $(shell git describe --abbrev=4 --dirty --always --tags)

GO ?= go

carbonserver-collector: dep
	$(MAKE) VERSION=$(VERSION) GO=$(GO) -C cmd/carbonserver-collector

flamegraph-server: dep
	$(MAKE) VERSION=$(VERSION) GO=$(GO) -C cmd/flamegraph-server

dep:
	@which dep 2>/dev/null || $(GO) get github.com/golang/dep/cmd/dep
	dep ensure

install:
	mkdir -p $(DESTDIR)/usr/bin/
	mkdir -p $(DESTDIR)/usr/share/carbonserver-flamegraphs/
	cp cmd/flamegraph-server/flamegraph-server $(DESTDIR)/usr/bin/
	cp cmd/carbonserver-collector/carbonserver-collector $(DESTDIR)/usr/bin/
	cp config.example.yaml $(DESTDIR)/usr/share/carbonserver-flamegraphs/

clean:
	rm -rf vendor
	rm -f cmd/carbonserver-collector/carbonserver-collector
	rm -f cmd/flamegraph-server/flamegraph-server
