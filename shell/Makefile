.PHONY: default dev dist packaging test testv deps updatedeps

default: dev

# dev creates your platform binary.
dev:
	@sh -c "$(CURDIR)/build/build.sh dev"

# dist creates all platform binaries.
dist:
	@sh -c "$(CURDIR)/build/build.sh dist"

# packaging creates all platform binaries and rpm packages.
packaging:
	@sh -c "$(CURDIR)/build/build.sh packaging"

# destroy remove all vagrant vm used to create packages.
destroy:
	@sh -c "$(CURDIR)/build/build.sh destroy"

test:
	go test ./... -cover

testv:
	go test ./... -v
