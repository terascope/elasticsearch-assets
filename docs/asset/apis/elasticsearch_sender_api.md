# elasticsearch_sender_api

The `elasticsearch_sender_api` makes the elasticsearch sender functionality available to any processor.   It's a [teraslice api](https://terascope.github.io/teraslice/docs/jobs/configuration#apis) that uses the [api factory](https://terascope.github.io/teraslice/docs/packages/job-components/api/operations/api-factory/overview) to create, cache, and manage multiple elasticsearch senders.   This api is the core of the [elasticsearch bulk](../operations/elasticsearch_bulk.md) operation and utilizes the standard metadata fields, e.g., `_key`, `_process_time`,`_ingest_time`, etc... See the [metadata section](#metadata) for details about metadata fields.

The elasticsearch_sender_api will also look for the metadata field `_delete_id` in each record's metadata, if this field exists it adds a delete operation for the id in the `_delete_id` field to the bulk request.  This allows for an index (or any other action) and a delete operation in the same bulk request.

## Usage

### Example using the elasticsearch sender API

A teraslice job and the associated processor using the elasticsearch_sender_api

Example Job

```json
{
    "name" : "example",
    "workers" : 1,
    "slicers" : 1,
    "lifecycle" : "once",
    "assets" : [
        "elasticsearch"
    ],
    "apis" : [
        {
            "_name": "elasticsearch_sender_api",
            "connection": "ELASTICSEARCH_CONNECTION",
            "index": "example_index",
            "type": "_doc",
            "size": 1000
        }
    ],
    "operations" : [
        {
            "_op" : "example-reader",
        },
         {
            "_op" : "example_sender",
            "api_name" : "elasticsearch_sender_api"
        },
    ]
}
```

The processor for the job described above

```javascript
// located at /example_sender/processor.ts

const { BatchProcessor } = require('@terascope/job-components');

export default class SomeSender extends BatchProcessor {
    async initialize() {
        await super.initialize();
        const apiManager = this.getAP(this.opConfig.api_name);
        this.client = await apiManager.create('bulkSender', {});
    }

    async onBatch(data) {
        if (data == null || data.length === 0) return data;

        await this.client.send(data);

        // NOTE: its important to return original data so operators afterwards can run
        return data;
    }
}
```

## Elasticsearch Sender Factory API Methods

### size

Returns the number of separate sender apis

### get

parameters:

- name: String

Fetches any sender api associated with the name provided

### getConfig

parameters:

- name: String

Fetches any sender api config associated with the name provided

### create (async)

parameters:

- name: String
- configOverrides: Check options below, optional

Creates an instance of a [sender api](#elasticsearch-sender-instance) and caches it with the name given. Any config provided in the second argument will override what is specified in the apiConfig. Throws an error if you try creating another api with the same name.

### remove (async)

parameters:

- name: String

Removes an instance of a sender api and follows any cleanup code specified in the api code.

### entries

Iterates over the cached names and clients

### keys

Iterates over the cached names

### values

Iterates over the values

## Example of using the factory methods in a processor

```javascript
// example of api configuration
const apiConfig = {
  _name: "elasticsearch_sender_api",
  index: "new_index",
  size: 1000,
  type: "events",
  connection: "default"
};


const apiManager = this.getAPI(apiName);

apiManager.size() === 0

// returns an api cached at "normalClient" and uses the default api config
const normalClient = await apiManager.create('normalClient', {})

apiManager.size() === 1

apiManager.get('normalClient') === normalClient

// returns an api cached at "overrideClient" and it will use the api config, but overrides the index to "other_index" in the new instance.
const overrideClient = await apiManager.create('overrideClient', { index: 'other_index', connection: "other", update: true })

apiManager.size() === 2

// returns the full configuration for this client
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

The sender class, [sender api](https://terascope.github.io/teraslice/docs/packages/utils/api/interfaces/interfaces/RouteSenderAPI/),  returned from the create method of the APIFactory, follows our common sender api interface.

### send (async)

```(records: DataEntities[]) => Promise<void>```
Formats an elasticsearch bulk request and sends it to elasticsearch

parameters:

- records: an array of data-entities

### verify (async)

```(route?: string) => Promise<void>```
Ensures that the index is created. The bulk index request will make the index if it doesn't exist so this function is not necessary for the Elasticsearch sender, but this might change in the future.

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

### Metadata

When the records are fetched from elasticsearch, metadata will be attached
based off of the what metadata elasticsearch results provides

- `_key` is set to the _id
- `_processTime` is set to a a number representing the milliseconds elapsed since the UNIX epoch of when it was first fetched
- `_ingestTime` is set to a a number representing the milliseconds elapsed since the UNIX epoch of when it was first fetched
- `_eventTime`  is set to a a number representing the milliseconds elapsed since the UNIX epoch of when it was first fetched
- `_index` is set from the index it was from
- `_type` is set to the records _type
- `_version` is set to the records _version_
- `_seq_no` is set to the records _seq_no parameter if it exists
- `_primary_term` is set to the records _primary_term parameter if it exists

Example of metadata from a fetched record

```javascript
// example record in elasticsearch
{
    "_index" : "test_index",
    "_type" : "_doc",
    "_id" : "ltyRQW4B8WLke7PkER8L",
    "_score" : 1.0,
    "_source" : {
      "ip" : "120.67.248.156",
      "url" : "http://lucious.biz",
      "uuid" : "a23a8550-0081-453f-9e80-93a90782a5bd",
      "created" : "2019-04-26T08:00:23.225-07:00",
      "ipv6" : "9e79:7798:585a:b847:f1c4:81eb:0c3d:7eb8",
      "location" : "50.15003, -94.89355",
      "bytes" : 124
    }
}

const expectedResults = {
    "ip" : "120.67.248.156",
    "url" : "http://lucious.biz",
    "uuid" : "a23a8550-0081-453f-9e80-93a90782a5bd",
    "created" : "2019-04-26T08:00:23.225-07:00",
    "ipv6" : "9e79:7798:585a:b847:f1c4:81eb:0c3d:7eb8",
    "location" : "50.15003, -94.89355",
    "bytes" : 124
};

DataEntity.isDataEntity(expectedResults) === true;

expectedResults.getMetadata() === {
    _key: "ltyRQW4B8WLke7PkER8L",
    _type: "_doc",
    _index: "test_index",
    _version: undefined,
    _seq_no: undefined,
    _primary_term: undefined,
    _processTime: 1596663162372,
    _ingestTime: 1596663162372,
    _eventTime: 1596663162372,
}
```
