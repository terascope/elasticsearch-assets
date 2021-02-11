#!/bin/bash

set -e

main() {
    local dest="/asset/node_modules/@terascope/elasticsearch-asset-apis"
    if [ -d "$dest" ]; then
        echo "* copying the files from elasticsearch-asset-apis"
        rm "$dest"
        cp -R ./packages/elasticsearch-asset-apis/* "$dest"
    fi
}

main "$@"
