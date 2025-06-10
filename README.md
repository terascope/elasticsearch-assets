# elasticsearch-assets

> A bundle of Teraslice processors for reading and writing elasticsearch data

## Getting Started

This asset bundle requires a running Teraslice cluster. [Documentation](https://github.com/terascope/teraslice/blob/master/README.md).

```bash
# Step 1: make sure you have teraslice-cli installed
yarn global add teraslice-cli

# Step 2:
teraslice-cli assets deploy <cluster-alias> --build
```

## APIS

* [Elasticsearch Reader API](./docs/asset/apis/elasticsearch_reader_api.md)
* [Elasticsearch Sender API](./docs/asset/apis/elasticsearch_sender_api.md)
* [Spaces Reader API](./docs/asset/apis/spaces_reader_api.md)
* [Elasticsearch State Storage](./docs/asset/apis/elasticsearch_state_storage.md)

## Operations

* [elasticsearch_reader](./docs/asset/operations/elasticsearch_reader.md)
* [elasticsearch_bulk](./docs/asset/operations/elasticsearch_bulk.md)
* [id_reader](./docs/asset/operations/id_reader.md)
* [spaces_reader](./docs/asset/operations/spaces_reader.md)

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](./LICENSE) licensed.
