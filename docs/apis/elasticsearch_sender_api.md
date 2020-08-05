# elasticsearch_sender_api

The `elasticsearch_sender_api` will provide a factory that can create sender apis that can be accessed in any operation through the `getAPI` method on the operation.


This is a [Factory API](https://terascope.github.io/teraslice/docs/packages/job-components/api/interfaces/apifactoryregistry), which can be used to fully manage api creation and configuration.


## Elasticsearch Sender Factory API Methods

### size

this will return how many separate sender apis are in the cache

### get
parameters:
- name: String

this will fetch any sender api that is associated with the name provided

### getConfig
parameters:
- name: String

this will fetch any sender api config that is associated with the name provided

### create (async)
parameters:
- name: String
- configOverrides: Check options below, optional

this will create an instance of a sender api, and cache it with the name given. Any config provided in the second argument will override what is specified in the apiConfig and cache it with the name provided. It will throw an error if you try creating another api with the same name parameter

```typescript
const apiManager = this.getAPI<ElasticSenderFactoryAPI>(apiName);
// this will return an api cached at "normalClient" and it will use the default api config
const client = apiManager.create('normalClient')

// this will return an api cached at "overrideClient" and it will use the api config but override the index to "other_index" in the new instance.
const overrideClient = apiManager.create('overrideClient', { index: 'other_index'})
```

### remove (async)
parameters:
- name: String

this will remove an instance of a sender api from the cache and will follow any cleanup code specified in the api code.

### entries

This will allow you to iterate over the cache name and client of the cache

### keys

This will allow you to iterate over the cache name of the cache

### values

This will allow you to iterate over the values of the cache


## Elasticsearch Sender Instance
This is the sender class that is returned from the create method of the APIFactory. This returns an [elastic-api](https://terascope.github.io/teraslice/docs/packages/elasticsearch-api/overview).

### send
```(records: DataEntities[]) => Promise<void>```
parameters:
- records: an array of data-entities


```js
await api.send([
    DataEntity.make({ some: 'data', name: 'someName', job: 'to be awesome!' })
]);
```

## Options


| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| \_op | Name of operation, it must reflect the exact name of the file | String | required |
| size | the maximum number of docs it will send in a given request, anything past it will be split up and sent | Number | required, typically the index selector returns up to double the length of the original documents due to the metadata involved with bulk requests. This number is essentially doubled to to maintain the notion that we split by actual documents and not the metadata |
| connection | Name of the elasticsearch connection to use when sending data | String | optional, defaults to the 'default' connection created for elasticsearch |
| index | Index to where the data will be sent to, it must be lowercase | String | required |
| type | Set the type of the data for elasticsearch | String | optional defaults to '_doc', is required for elasticsearch v5|
| delete| Use the id_field from the incoming records to bulk delete documents | Boolean | optional, defaults to false |
| upsert| Specify if the incoming records should be used to perform an upsert. If update_fields is also specified then existing records will be updated with those fields otherwise the full incoming  record will be inserted | Boolean | optional, defaults to false |
| create| Specify if the incoming records should be used to perform an create event ("put-if-absent" behavior)| Boolean | optional, defaults to false |
| update | Specify if the data should update existing records, if false it will index them | Boolean | optional, defaults to false |
| update_fields | if you are updating the documents, you can specify fields to update here (it should be an array containing all the field names you want), it defaults to sending the entire document | Array | optional, defaults to [] |
| script_file | Name of the script file to run as part of an update request | String | optional |
| script | Inline script to include in each indexing request. Only very simple painless scripts are currently supported | String | optional |
| script_params | key -> value parameter mappings. The value will be extracted from the incoming data and passed to the script as param based on the key | Object | optional |
| update_retry_on_conflict | If there is a version conflict from an update how often should it be retried | Number | optional, defaults to 0 |



### Example Processor using a elasticsearch sender API
```typescript
export default class SomeSender extends BatchProcessor<ElasticsearchBulkConfig> {
    client!: ElasticsearchSender;
    apiManager!: ElasticSenderAPI;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiManager = this.getAPI<ElasticSenderAPI>(this.opConfig.api_name);
        this.client = await apiManager.create('bulkSender', this.opConfig);
    }

    async onBatch(data: DataEntity[]): Promise<DataEntity[]> {
        if (data == null || data.length === 0) return data;
        await this.client.send(data);
        // NOTE: its important to return original data so operators afterwards can run
        return data;
    }
}
```
