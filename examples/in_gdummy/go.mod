module github.com/fluent/fluent-bit-go/examples/gdummy

go 1.21

toolchain go1.21.0

require github.com/calyptia/plugin v0.1.6

require (
	github.com/calyptia/cmetrics-go v0.1.7 // indirect
	github.com/ugorji/go/codec v1.2.11 // indirect
)

replace github.com/calyptia/plugin => ../..
