all:
	go build -buildmode=c-shared -o custom_extensions.so .

fast:
	go build custom_extensions.go

clean:
	rm -rf *.so *.h *~
