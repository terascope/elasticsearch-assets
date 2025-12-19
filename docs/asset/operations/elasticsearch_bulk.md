# elasticsearch_bulk

The elasticsearch_bulk operator is a high throughput bulk sender to an elasticsearch index.

There are four types of [bulk requests](https://www.elastic.co/guide/en/elasticsearch/reference/7.x/docs-bulk.html#docs-bulk-api-desc): index, create, update and delete.

This operation requires that the incoming data-entities to this processors
have a `_key` metadata field set to the id of the record for `update`, `create` and `delete` requests.

Although not needed for `index` bulk requests, setting the `_key` on the record will create the new record with that id as opposed to one that is automatically generated for you.

When using the elasticsearch_reader or the elasticsearch_reader_api to fetch records, the `_key` will automatically be set to the elasticsearch records `_id`.
You can use other processors to remove or alter that if it is not wanted.

## Usage

### Send index batch request, setting the _id of the records

By default we make an index bulk request, since the records have a `_key`, that is used as the new elasticsearch _id of the record

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
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "connection": "es-1",
            "index": "test_index",
            "date_field_name": "created"
        },
        {
            "_op": "elasticsearch_bulk",
            "index": "new_index",
            "type": "events"
        }
    ]
}
```

Below is a representation of the incoming data and the resulting bulk request being made to elasticsearch

```javascript

const records = [
    new DataEntity({ some: 'data' }, { _key: '1234' }),
    new DataEntity({ other: 'stuff' }, { _key: '5678' })
]

// will be converted to this bulk request

[
    { index: { _index: 'new_index', _id: '1234' }},
    { some: 'data' },
    { index: { _index: 'new_index', _id: '5678' }},
    { other: 'stuff' }
]

```

### Send an update batch request, only updating selected fields

We can make an update batch request and limit what fields are being updated, in this job, only the name and job fields will be updated

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
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "index": "test_index",
            "date_field_name": "created",
            "connection": "es-1"
        },
        {
            "_op": "elasticsearch_bulk",
            "index": "new_index",
            "type": "events",
            "update": true,
            "update_fields": ["name", "job"]
        }
    ]
}
```

Below is a representation of the incoming data and the resulting bulk request being made to elasticsearch

```javascript

const records = [
    new DataEntity({ some: 'data', name: 'someName', job: 'to be awesome!' }, { _key: '1234' }),
]

// will be converted to this bulk request

[
    { update: { _index: 'new_index', _id: '1234' }},
    { doc: { name: 'someName', job: 'to be awesome!' } },
]

```

### Send upsert batch request and use scripts to make additional changes

By default we make an index bulk request, since the records have a `_key`, that is used as the new elasticsearch _id of the record

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
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "index": "test_index",
            "date_field_name": "created",
            "connection": "es-1"
        },
        {
            "_op": "elasticsearch_bulk",
            "index": "new_index",
            "type": "events",
            "upsert": true,
            "script": "ctx._source.count += add",
            "script_params": { "add": "add" }
        }
    ]
}
```

Below is a representation of the incoming data and the resulting bulk request being made to elasticsearch

```javascript

const records = [
    new DataEntity({ count: 1, add: 2  }, { _key: '1234' }),
]

// will be converted to this bulk request

[
    { update: { _index: 'new_index', _id: '1234' }},
    {
        upsert: { count: 1, add: 2  },
        script: {
            source: 'ctx._source.count += add',
            params: {
                add: 2
            }
        }
    }
]

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
| _api_name                | name of api to be used by elasticsearch bulk sender                                                                                                                                                                  | String  | optional, defaults to 'elasticsearch_sender_api'                                                                                                                                                                                                                      |

### API usage in a job

In elasticsearch_assets v3, many core components were made into teraslice apis. When you use an elasticsearch processor it will automatically setup the api for you, but if you manually specify the api, then there are restrictions on what configurations you can put on the operation so that clashing of configurations are minimized. The api configs take precedence.

If submitting the job in long form, here is a list of parameters that will throw an error if also specified on the opConfig, since these values should be placed on the api:

- `index`

`SHORT FORM (no api specified)`

```json
{
    "name" : "testing",
    "workers" : 1,
    "slicers" : 1,
    "lifecycle" : "once",
    "assets" : [
        "elasticsearch"
    ],
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "index": "test_index",
            "field": "uuid",
            "size": 1000,
            "key_type": "base64url"
        },
        {
            "_op": "elasticsearch_bulk",
            "index": "other_index",
            "size": 1000,
            "type": "events"
        }
    ]
}

```

this configuration will be expanded out to the long form underneath the hood
`LONG FORM (api is specified)`

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
            "_name": "elasticsearch_reader_api",
            "index": "test_index",
            "field": "uuid",
            "size": 1000,
            "key_type": "base64url",
            "connection": "default"
        },
        {
            "_name": "elasticsearch_sender_api",
            "index": "other_index",
            "size": 1000,
            "type": "events",
            "connection": "default"
        }
    ],
    "operations" : [
        {
            "_op" : "id_reader",
            "api_name" : "elasticsearch_reader_api"
        },
         {
            "_op": "elasticsearch_bulk",
            "api_name" : "elasticsearch_sender_api"
        }
    ]
}
```

### Dead Letter Queue Support

The elasticsearch_bulk processor supports the [dead letter queue api](https://terascope.github.io/kafka-assets/docs/asset/apis/kafka_dead_letter) as of version `3.5.0`.  When the dead_letter_queue functionality is active records that are rejected by elasticsearch with a `_bulk_sender_rejection` error are forwarded to the kafka topic specified in the dead letter queue api configs. Records that do not have the error are still written to the designated cluster as usual.

To trigger this behavior add the property and value `_dead_letter_action: kafka_dead_letter` to the `elasticsearch_bulk` _op configs.

Example Job:

```json
{
    "name" : "testing",
    "workers" : 1,
    "slicers" : 1,
    "lifecycle" : "once",
    "assets" : [
        "elasticsearch"
    ],
    "apis": [
         {
            "_name": "kafka_dead_letter",
            "connection": "KAFKA_CONNECTION",
            "topic": "KAFKA_TOPIC",
            "size": 10000
        }
    ]
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "index": "INDEX_NAME",
            "date_field_name": "created",
            "connection": "ES_CLUSTER_CONNECTION"
        },
        {
            "_op": "elasticsearch_bulk",
            "connection": "ES_CLUSTER_CONNECTION",
            "index": "INDEX_NAME",
            "type": "events",
             "_dead_letter_action": "kafka_dead_letter"
        }
    ]
}
```
