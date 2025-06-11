# elasticsearch_state_storage

This is a [teraslice api](https://terascope.github.io/teraslice/docs/jobs/configuration#apis), which encapsulates a specific functionality that can be utilized by any processor, reader or slicer.

The `elasticsearch_state_storage` will provide a [state-storage api](https://terascope.github.io/teraslice/docs/packages/teraslice-state-storage/overview)

This api provides an LRU caching system, based on [mnemonist's](https://www.npmjs.com/package/mnemonist) LRU map, for teraslice processors. The in memory cache is backed by Elasticsearch.

The advantage of having the LRU cache backed by a persistent storage system like Elasticsearch is that if the key is not in the cache the processor will search an elasticsearch index for the key and if it is found will add it to the cache.  This essentially expands the cache to the size of the underlying elasticsearch index without requiring the same memory resources in Teraslice.

The potential drawback is that on data sets with a large key set the processor will be continuously searching elasticsearch for each key which would render the caching mechanism pointless.

## Usage

### Example Processor using the elasticsearch state storage API

This is an example of a processor using the elasticsearch_state_storage api to cache records that have a later time field value

Example job

```json
 {
    "name": "es-state-storage-job",
    "lifecycle": "persistent",
    "workers": 20,
    "assets": [ "elasticsearch"],
     "apis": [
        {
            "_name": "elasticsearch_state_storage",
            "connection": "ELASTICSEARCH_CLUSTER_URL",
            "index": "INDEX_NAME",
            "type": "ELASTICSEARCH_TYPE",
            "cache_size": 1000000
        }
    ],
    "operations": [
        {
            "_op": "reader"
        },
        {
            "_op": "state_storage_processor",
            "api_name": "elasticsearch_state_storage"
        },
        {
            "_op": "sender"
        }
    ]
}
```

This processor is comparing the `time` values of each record and caching them

```typescript

export default class StateStorageProcessor extends BatchProcessor {
    api!: ESCachedStateStorage;

    async initialize() {
        await super.initialize();
        this.api = this.getAPI<ElasticsearchStateStorage>(this.opConfig.api_name);;
    }


    compareRecord(incData:DataEntity, cachedRecord:DataEntity) {
        return incData.time > cachedRecord.time
    }

    async onBatch(data: DataEntity[]): Promise<DataEntity[]> {
        const results: DataEntity[] = [];
        const setRecords: DataEntity[] = [];

        for (const record of data) {
            if (!this.api.isCached(record)) {
                setRecords.push(record);
                continue;
            }

            if (this.compareRecord(record, this.api.getFromCache(record))) {
                this.api.set(record);
                results.push(record);
            }
        }

        if (fetchRecords.length > 0) await this.stateStorage.mset(fetchRecords);

        return results;
    }
}
```

## Elasticsearch State Storage API

Elasticsearch State Storage operates under the assumption that all records being processed are data entities

```javascript
const foo = DataEntity.make({ name: 'foo'}, { _key: 1 });
const bar = DataEntity.make({ name: 'bar'}, { _key: 2 });
```

### set

set(DATAENTITY) - Adds the records to the cache. If the cache is already full, the least recently used key will be dropped from the cache and the evicted value will be logged by teraslice

```javascript
api.set(foo);
api.set(bar);
```

### get (async)

get(DATAENTITY) - Asynchronous function that returns the cached state of the input.  If the record is not cached then it will search the elasticsearch index for the record.  If the record is found, the key is moved to the front of the underlying list to be the most recently used item.

```javascript
api.get(foo); // { name: 'foo' }
```

### mset (async)

mset([DATAENTITY1, DATAENTITY2, etc...]) - Asynchronous function that adds records to the cache. If persist is true it will also save the records in the elasticsearch index.  Input is a data entity array.

```javascript
api.mset([foo, bar]);
```

### mget (async)

mget([DATAENTITY1, DATAENTITY2, etc...]) - Asynchronous function that returns an object of the cached keys and values.  For records not in the cache it will search elasticsearch and add found records to the cache.  Input is data entity array

```javascript
api.mget([foo, bar]); // { 1: { name: 'foo' }, 2: { name: 'bar' } };
```

### isCached

isCached(DATA_ENTITY) - Return true if the records key is in the cache otherwise returns false

```javascript
api.isCached(foo); // true
api.isCached(other); // false
```

### isKeyCached

isKeyCached(KEY) - Returns true if key is in the cache otherwise returns false.

```javascript
api.isKeyCached(1); // true
api.isKeyCached('other'); // false
```

### count

count - Returns the number of records in the cache

```javascript
api.count(); // 2
```

## Cache Functions

If you need fine grain control over the LRU cache, you can access it at `api.cache`. Many of these methods are already called by the elasticsearch state storage methods. Here are a list of methods on the cache

### set

set(KEY, VALUE) - Sets a value for the given key in the cache. If the cache is already full, the least recently used key will be dropped from the cache and the evicted value will be logged by teraslice

```javascript
api.cache.set(1, { name: 'foo' });
api.cache.set('abc123', { name: 'bar' });
```

### get

get(KEY) - Retrieves the value associated to the given key in the cache or undefined if the key is not found.  If the key is found, the key is moved to the front of the underlying list to be the most recently used item.

```javascript
api.cache.get(1); // { name: 'foo' }
api.cache.get('abc123'); // { name: 'bar' }
api.cache.get('456def'); // undefined
```

### mset

`mset([{ key: KEY1, data: VALUE1}, { key: KEY2, data: VALUE2 }, etc ...]` - Sets multiple key, value pairs.  Requires an array of `{ key: key, data: value }` objects

```javascript
api.cache.mset([{ key: 1, data: { name: 'foo' } }, { key: 'abc123', data: { name: 'bar' } }]);
```

### mget

mget([KEY1, KEY2, KEY3, etc...]) - Returns an object of the found keys and values.  Required input is an array of keys

```javascript
api.cache.mget([1, 'abc123', '456def']); // { 1: { name: 'foo' }, 'abc123': { name: 'bar' } };
```

### values

values(function) - Processes cache values based on passed function.

```javascript
    const results = [];

   function processValues(data) {
       results.push(data);
   }

   api.cache.values(processValues); // [ { name: 'foo' }, { name: 'bar' }];
```

### has

has(KEY) - Returns true if key is in the cache otherwise returns false.

```javascript
api.cache.has(1); // true
api.cache.has('345def'); // false
```

### clear

`clear() - Completely clears the cache.`

```javascript
api.cache.clear();
```

## Parameters

| Configuration | Description | Type |  Notes   |
| --------- | -------- | ------ | ------ |
| \_name | Name of operation, it must reflect the exact name of the file | String | required |
| cache_size | Maximum number of keys held in the cache before evicting unused keys  | Number | optional, defaults to 2,147,483,647 |
| index | Which index to read from | String | required |
| type | The type of the document that you are reading, used when a chuck is so large that it must be divided up by the documents \_id|String | required if using elasticsearch v5, optional otherwise, defaults to '_doc' |
| concurrency | Number of concurrent elasticsearch mget requests | Number | optional, defaults to 10 |
| chunk_size | Number of documents in each elasticsearch mget request | Number | optional,defaults to 2,500 |
| persist | Saves the record to elasticsearch upon caching the document | Number | optional, defaults to false |
| meta_key_field | Field in the metadata to use as the key for caching and searching in elasticsearch | String | optional, defaults to "_key" |
| connection | Name of the elasticsearch connection to use when sending data | String | optional, defaults to the 'default' connection created for elasticsearch |
