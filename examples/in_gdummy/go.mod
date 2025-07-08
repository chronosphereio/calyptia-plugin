module github.com/fluent/fluent-bit-go/examples/gdummy

go 1.23.0

toolchain go1.24.2

require github.com/calyptia/plugin v0.1.6

require (
	github.com/calyptia/cmetrics-go v0.1.7 // indirect
	github.com/ugorji/go/codec v1.2.12 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
)

replace github.com/calyptia/plugin => ../..
