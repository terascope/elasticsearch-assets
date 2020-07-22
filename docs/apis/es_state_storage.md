# elasticsearch_state_storage

The `elasticsearch_state_storage` will provide a [state-storage api](https://github.com/terascope/teraslice/blob/master/docs/packages/teraslice-state-storage/overview.md)

please refer to that guide for api references

## Options

| Configuration | Description | Type |  Notes   |
| --------- | -------- | ------ | ------ |
| \_name | Name of operation, it must reflect the exact name of the file | String | required |
| cache_size | Maximum number of keys held in the cache before evicting unused keys  | Number | optional, defaults to 2,147,483,647 |
| index | Which index to read from | String | required |
| type | The type of the document that you are reading, used when a chuck is so large that it must be divided up by the documents \_id|String | required if using elasticsearch v5, optional otherwise, defaults to '_doc' |
| concurrency | Number of concurrent elasticsearch mget requests | Number | optional, defaults to 10 |
| chunk_size | Number of documents in each elasticsearch mget request | optional,defaults to 2,500 |
| persist | Saves the record to elasticsearch upon caching the document | optional, defaults to false |
| meta_key_field | Field in the metadata to use as the key for cacheing and searching in elasticsearch | String | optional, defaults to "_key" |
| connection | Name of the elasticsearch connection to use when sending data | String | optional, defaults to the 'default' connection created for elasticsearch |


### Example Processor using a elasticsearch reader api
```typescript

export default class SomeStorage extends OperationAPI {
    stateStorage: ESCachedStateStorage;

    constructor(
        context: WorkerContext,
        apiConfig: ESStateStorageConfig,
        executionConfig: ExecutionConfig
    ) {
        super(context, apiConfig, executionConfig);
        const { client } = this.context.foundation.getConnection({
            endpoint: this.apiConfig.connection,
            type: 'elasticsearch',
            cached: true
        });
        // @ts-expect-error
        this.stateStorage = new ESCachedStateStorage(client, this.logger, this.apiConfig);
    }

    async initialize(): Promise<void> {
        await super.initialize();
        await this.stateStorage.initialize();
    }

    async shutdown(): Promise<void> {
        await super.shutdown();
        await this.stateStorage.shutdown();
    }

    async createAPI(): Promise<ESCachedStateStorage> {
        return this.stateStorage;
    }
}
```
