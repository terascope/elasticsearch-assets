# elasticsearch-assets

> A bundle of Teraslice processors for reading and writing elasticsearch data

## Getting Started

This asset bundle requires a running Teraslice cluster, you can find the documentation [here](https://github.com/terascope/teraslice/blob/master/README.md).

```bash
# Step 1: make sure you have teraslice-cli installed
yarn global add teraslice-cli

# Step 2:
teraslice-cli assets deploy <cluster-alias> --build

```
## APIS

 * [Elasticsearch Reader API](./docs/apis/es_reader.md)
 * [Elasticsearch Sender API](./docs/apis/es_sender.md)
 * [Spaces Reader API](./docs/apis/spaces_reader.md)
 * [Elasticsearch State Storage](./docs/apis/es_state_storage.md)


## Operations
 * [elasticsearch_reader](./docs/operations/es_reader.md)
 * [elasticsearch_bulk](./docs/operations/es_bulk.md)
 * [elasticsearch_data_generator](./docs/operations/data_generator.md)
 * [id_reader](./docs/operations/id_reader.md)
 * [spaces_reader](./docs/operations/spaces_reader.md)

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](./LICENSE) licensed.
