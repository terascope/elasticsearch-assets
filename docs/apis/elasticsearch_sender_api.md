# elasticsearch_sender_api

This is a [teraslice api](https://terascope.github.io/teraslice/docs/jobs/configuration#apis), which encapsulates a specific functionality that can be utilized by any processor, reader or slicer.

 The `elasticsearch_sender_api` will provide an [api factory](https://terascope.github.io/teraslice/docs/packages/job-components/api/classes/apifactory), which is a singleton that can create, cache and manage multiple elasticsearch readers that can be accessed in any operation through the `getAPI` method on the operation.

This api is the core of the [elasticsearch_bulk](../operations/elasticsearch_bulk.md). This contains all the same behavior, functionality and configuration of that reader

## Usage
### Example Processor using a elasticsearch sender API
This is an example of a custom fetcher using the elasticsearch_reader_api to make its own queries to elasticsearch.

Example Job

```json
{
    "name" : "testing",
    "workers" : 1,
    "slicers" : 1,
    "lifecycle" : "once",
    "assets" : [
        "elasticsearch"
    ],
    "apis" : [
        {
            "_name": "elasticsearch_sender_api",
            "index": "new_index",
            "size": 1000,
            "type": "events",
            "connection": "default"
        }
    ],
    "operations" : [
        {
            "_op" : "test-reader",
        },
         {
            "_op" : "some_sender",
            "api_name" : "elasticsearch_sender_api"
        },
    ]
}
```
Here is a custom processor for the job described above

```typescript
// located at /some_sender/processor.ts

export default class SomeSender extends BatchProcessor<ElasticsearchBulkConfig> {
    client!: ElasticsearchBulkSender;
    apiManager!: ElasticSenderAPI;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiManager = this.getAPI<ElasticSenderAPI>(this.opConfig.api_name);
        this.client = await apiManager.create('bulkSender', {});
    }

    async onBatch(data: DataEntity[]): Promise<DataEntity[]> {
        if (data == null || data.length === 0) return data;
        await this.client.send(data);
        // NOTE: its important to return original data so operators afterwards can run
        return data;
    }
}
```

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

this will create an instance of a [sender api](#elasticsearch_sender_instance), and cache it with the name given. Any config provided in the second argument will override what is specified in the apiConfig and cache it with the name provided. It will throw an error if you try creating another api with the same name parameter

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


## Example of using the factory methods in a processor
```typescript
// example of api configuration
const apiConfig = {
  _name: "elasticsearch_sender_api",
  index: "new_index",
  size: 1000,
  type: "events",
  connection: "default"
};


const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);

apiManager.size() === 0

// this will return an api cached at "normalClient" and it will use the default api config
const normalClient = await apiManager.create('normalClient', {})

apiManager.size() === 1

apiManager.get('normalClient') === normalClient

// this will return an api cached at "overrideClient" and it will use the api config but override the index to "other_index" in the new instance.
const overrideClient = await apiManager.create('overrideClient', { index: 'other_index', connection: "other", update: true })

apiManager.size() === 2

// this will return the full configuration for this client
apiManger.getConfig('overrideClient') === {
  _name: "elasticsearch_sender_api",
  index: "other_index",
  size: 1000,
  type: "events",
  connection: "other",
  update: true
}


await apiManger.remove('normalClient');

apiManager.size() === 1

apiManager.get('normalClient') === undefined

```

## Elasticsearch Sender Instance
This is the sender class that is returned from the create method of the APIFactory. This returns a [sender api](https://terascope.github.io/teraslice/docs/packages/job-components/api/interfaces/routesenderapi), which is a common interface used for sender apis.

### send (async)
```(records: DataEntities[]) => Promise<void>```
This method will format the records into an elasticsearch bulk request and send them to elasticsearch

parameters:
- records: an array of data-entities

### verify (async)
```(route?: string) => Promise<void>```
This method ensures that the index is created. However, this currently is a noop as the bulk index request will make the index, this might change in the future. This exists because this follows a common interface. Other senders might need to verify that the destination exists before sending data.

parameters:
- route: a string representing the index to create


### Usage of the elasticsearch sender instance
```js
await api.send([
    DataEntity.make({
        some: 'data',
        name: 'someName',
        job: 'to be awesome!'
    })
]);
```

## Parameters

| Configuration            | Description                                                                                                                                                                                                          | Type    | Notes                                                                                                                                                                                                                                                                 |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| \_op                     | Name of operation, it must reflect the exact name of the file                                                                                                                                                        | String  | required                                                                                                                                                                                                                                                              |
| size                     | the maximum number of docs it will send in a given request, anything past it will be split up and sent                                                                                                               | Number  | required, typically the index selector returns up to double the length of the original documents due to the metadata involved with bulk requests. This number is essentially doubled to to maintain the notion that we split by actual documents and not the metadata |
| connection               | Name of the elasticsearch connection to use when sending data                                                                                                                                                        | String  | optional, defaults to the 'default' connection created for elasticsearch                                                                                                                                                                                              |
| index                    | Index to where the data will be sent to, it must be lowercase                                                                                                                                                        | String  | required                                                                                                                                                                                                                                                              |
| type                     | Set the type of the data for elasticsearch                                                                                                                                                                           | String  | optional defaults to '_doc', is required for elasticsearch v5                                                                                                                                                                                                         |
| delete                   | Use the id_field from the incoming records to bulk delete documents                                                                                                                                                  | Boolean | optional, defaults to false                                                                                                                                                                                                                                           |
| upsert                   | Specify if the incoming records should be used to perform an upsert. If update_fields is also specified then existing records will be updated with those fields otherwise the full incoming  record will be inserted | Boolean | optional, defaults to false                                                                                                                                                                                                                                           |
| create                   | Specify if the incoming records should be used to perform an create event ("put-if-absent" behavior)                                                                                                                 | Boolean | optional, defaults to false                                                                                                                                                                                                                                           |
| update                   | Specify if the data should update existing records, if false it will index them                                                                                                                                      | Boolean | optional, defaults to false                                                                                                                                                                                                                                           |
| update_fields            | if you are updating the documents, you can specify fields to update here (it should be an array containing all the field names you want), it defaults to sending the entire document                                 | Array   | optional, defaults to []                                                                                                                                                                                                                                              |
| script_file              | Name of the script file to run as part of an update request                                                                                                                                                          | String  | optional                                                                                                                                                                                                                                                              |
| script                   | Inline script to include in each indexing request. Only very simple painless scripts are currently supported                                                                                                         | String  | optional                                                                                                                                                                                                                                                              |
| script_params            | key -> value parameter mappings. The value will be extracted from the incoming data and passed to the script as param based on the key                                                                               | Object  | optional                                                                                                                                                                                                                                                              |
| update_retry_on_conflict | If there is a version conflict from an update how often should it be retried                                                                                                                                         | Number  | optional, defaults to 0                                                                                                                                                                                                                                               |
