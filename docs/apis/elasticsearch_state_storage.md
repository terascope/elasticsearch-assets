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
| meta_key_field | Field in the metadata to use as the key for caching and searching in elasticsearch | String | optional, defaults to "_key" |
| connection | Name of the elasticsearch connection to use when sending data | String | optional, defaults to the 'default' connection created for elasticsearch |


### Example Processor using the elasticsearch state storage API
```typescript

export default class SomeStorage extends BatchProcessor {
    stateStorage!: ESCachedStateStorage;

    async initialize() {
        await super.initialize();
        this.stateStorage = this.getAPI<ElasticsearchStateStorage>(this.opConfig.api_name);;
    }

     async shutdown() {
        await super.initialize();
        this.stateStorage = this.getAPI<ElasticsearchStateStorage>(this.opConfig.api_name);;
    }

    compareRecord(incData:DataEntity, cachedRecord:DataEntity) {
        return incData.time > cachedRecord.time
    }

    async onBatch(data: DataEntity[]): Promise<DataEntity[]> {
        const results: DataEntity[] = [];
        const setRecords: DataEntity[] = [];

        for (const record of data) {
            if (!this.stateStorage.isCached(record)) {
                setRecords.push(record);
                continue;
            }

            if (this.compareRecord(record, this.stateStorage.getFromCache(record))) {
                this.stateStorage.set(record);
                results.push(record);
            }
        }

        if (fetchRecords.length > 0) await this.stateStorage.mset(fetchRecords);

        return results;
    }
}
```
