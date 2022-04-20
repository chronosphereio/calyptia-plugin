# Local, run them only if host is linux/amd64 and you have C++ build tools.

local-input-plugin:
	go build -trimpath -buildmode c-shared -o ./plugin/testdata/go-test-input-plugin.so ./plugin/testdata/input/input.go

local-output-plugin:
	go build -trimpath -buildmode c-shared -o ./plugin/testdata/go-test-output-plugin.so ./plugin/testdata/output/output.go

local-plugins: local-input-plugin local-output-plugin

fluentbit-run:
	docker run --rm -v $(shell pwd)/plugin/testdata:/fluent-bit/etc/:ro ghcr.io/calyptia/enterprise/advanced:main /fluent-bit/bin/fluent-bit -c fluent-bit/etc/fluent-bit.conf

# -------------------------------------------------------------------------------------------
# Using docker, run these if you are not in linux/amd64 or if you don't have C++ build tools.
# -------------------------------------------------------------------------------------------

docker-image:
	docker build . -t plugin-test --platform linux/amd64

docker-image-run:
	docker run --rm --platform linux/amd64 plugin-test
