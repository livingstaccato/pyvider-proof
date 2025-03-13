#!/bin/sh
# pyvider-rpcplugin/examples/kvprobo/go-plugin/build.sh

set -e # Exit on any error

export PLUGIN_CLIENT_PATH="$(pwd)/bin/kv-go-client"
export PLUGIN_SERVER_PATH="$(pwd)/bin/kv-go-server"

echo "Cleaning up previous builds..."
rm -f ${PLUGIN_CLIENT_PATH} ${PLUGIN_SERVER_PATH}

# Initialize module if needed
if [ ! -f go.mod ]; then
	echo "Initializing Go module..."
	go mod init github.com/provide-io/pyvider-rpcplugin/examples/kvprobo/go-plugin

	echo "Installing buf dependencies..."
	go install github.com/bufbuild/buf/cmd/buf@latest

	echo "Generating protobuf code..."
	buf generate
fi

echo "Updating Go dependencies..."
go mod tidy

echo "Building client and server..."
go build -o ${PLUGIN_CLIENT_PATH} ./plugin-go-client
go build -o ${PLUGIN_SERVER_PATH} ./plugin-go-server

echo "Build complete. Binary information:"
file ${PLUGIN_CLIENT_PATH}
file ${PLUGIN_SERVER_PATH}

echo "\nNext steps:"
echo "1. Set environment variables: source env.sh"
echo "2. Run tests: ./test.sh"
