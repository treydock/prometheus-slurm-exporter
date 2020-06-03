include Makefile.common

test:
	GO111MODULE=$(GO111MODULE) $(GO) test -v ./...
