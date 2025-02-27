# elasticsearch-assets

> A bundle of Teraslice processors for reading and writing elasticsearch data

## Getting Started

TEST

This asset bundle requires a running Teraslice cluster, you can find the documentation [here](https://github.com/terascope/teraslice/blob/master/README.md).

```bash
# Step 1: make sure you have teraslice-cli installed
yarn global add teraslice-cli

# Step 2:
teraslice-cli assets deploy <cluster-alias> --build
```

## APIS

* [Elasticsearch Reader API](./docs/apis/elasticsearch_reader_api.md)
* [Elasticsearch Sender API](./docs/apis/elasticsearch_sender_api.md)
* [Spaces Reader API](./docs/apis/spaces_reader_api.md)
* [Elasticsearch State Storage](./docs/apis/elasticsearch_state_storage.md)

## Operations

* [elasticsearch_reader](./docs/operations/elasticsearch_reader.md)
* [elasticsearch_bulk](./docs/operations/elasticsearch_bulk.md)
* [elasticsearch_data_generator](./docs/operations/elasticsearch_data_generator.md)
* [id_reader](./docs/operations/id_reader.md)
* [spaces_reader](./docs/operations/spaces_reader.md)

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](./LICENSE) licensed.
