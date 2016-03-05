.PHONY: default dev dist packaging test testv deps updatedeps

BOLT_HASH = 2f846c3551b76d7710f159be840d66c3d064abbe

default: dev

# dev creates your platform binary.
dev:
	@cd shell && $(MAKE) $@
	@echo "--> Building demo todo app."
	@go build -o todo demo/todo/todo.go
	@echo "--> Done."

# dist creates all platform binaries.
dist:
	@cd shell && $(MAKE) $@

# packaging creates all platform binaries and rpm packages.
packaging:
	@cd shell && $(MAKE) $@

# destroy remove all vagrant vm used to create packages.
destroy:
	@cd shell && $(MAKE) $@

test:
	cd shell && go get && cd -
	go test . ./shell/... -cover

testv:
	cd shell && go get && cd -
	go test . ./shell/... -v

deps:
	rm -rf vendor
	git clone https://github.com/boltdb/bolt vendor/bolt
	cd vendor/bolt && git checkout $(BOLT_HASH) && rm -rf .git
