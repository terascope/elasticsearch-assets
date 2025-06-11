# elasticsearch-assets

> A bundle of Teraslice processors for reading and writing elasticsearch data

## Documentation

[https://terascope.github.io/elasticsearch-assets/](https://terascope.github.io/elasticsearch-assets/)

## Getting Started

This asset bundle requires a running Teraslice cluster. [Documentation](https://github.com/terascope/teraslice/blob/master/README.md).

```bash
# Step 1: make sure you have teraslice-cli installed
yarn global add teraslice-cli

# Step 2:
teraslice-cli assets deploy <cluster-alias> --build
```

## APIS

* [Elasticsearch Reader API](./docs/asset/apis/elasticsearch_reader_api)
* [Elasticsearch Sender API](./docs/asset/apis/elasticsearch_sender_api)
* [Spaces Reader API](./docs/asset/apis/spaces_reader_api)
* [Elasticsearch State Storage](./docs/asset/apis/elasticsearch_state_storage)

## Operations

* [elasticsearch_reader](./docs/asset/operations/elasticsearch_reader)
* [elasticsearch_bulk](./docs/asset/operations/elasticsearch_bulk)
* [id_reader](./docs/asset/operations/id_reader)
* [spaces_reader](./docs/asset/operations/spaces_reader)

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](./LICENSE) licensed.
