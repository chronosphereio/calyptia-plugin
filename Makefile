image:
	docker build . -t plugin-test --platform linux/amd64

run:
	docker run --rm --platform linux/amd64 plugin-test
