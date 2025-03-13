// examples/kvprobo/go-plugin/go.mod

module github.com/provide-io/pyvider-rpcplugin/examples/kvprobo/go-plugin

go 1.23.4

require (
	github.com/hashicorp/go-hclog v1.6.3
	github.com/hashicorp/go-plugin v1.6.3
	google.golang.org/grpc v1.69.2
	google.golang.org/protobuf v1.36.2
)

replace github.com/hashicorp/go-plugin => github.com/livingstaccato/go-plugin v0.0.0-20250305031206-470b1c194de6

require (
	github.com/fatih/color v1.13.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/hashicorp/yamux v0.1.1 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/oklog/run v1.0.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53 // indirect
)
