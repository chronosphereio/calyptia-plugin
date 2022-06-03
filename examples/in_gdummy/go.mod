module github.com/fluent/fluent-bit-go/examples/gdummy

go 1.17

require (
	github.com/calyptia/cmetrics-go v0.1.6
	github.com/calyptia/plugin v0.1.1
)

require github.com/ugorji/go/codec v1.2.7 // indirect

replace github.com/calyptia/plugin => ../..
