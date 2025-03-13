#!/bin/sh

export PLUGIN_CLIENT_PATH="$(pwd)/bin/kv-go-client"
export PLUGIN_SERVER_PATH="$(pwd)/bin/kv-go-server"

file ${PLUGIN_CLIENT_PATH}
file ${PLUGIN_SERVER_PATH}

echo "-------------------------------------------------------------------------"
echo "Putting the world into hello."
echo "-------------------------------------------------------------------------"
${PLUGIN_CLIENT_PATH} put hello world

echo "-------------------------------------------------------------------------"
echo "Fetching the value of the hello."
echo "-------------------------------------------------------------------------"
${PLUGIN_CLIENT_PATH} get hello
