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
    "apis" : [
         {
            "_name": "elasticsearch_reader_api",
            "_connection": "es-1",
            "index": "test_index",
            "date_field_name": "created"
        },
        {
            "_name": "elasticsearch_sender_api",
            "index": "new_index",
        }
    ],
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "_api_name" : "elasticsearch_reader_api"
        },
        {
            "_op": "elasticsearch_bulk",
            "_api_name" : "elasticsearch_sender_api"
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
    "apis" : [
         {
            "_name": "elasticsearch_reader_api",
            "index": "test_index",
            "date_field_name": "created",
            "_connection": "es-1"
        },
        {
            "_name": "elasticsearch_sender_api",
            "index": "new_index",
            "type": "events",
            "update": true,
            "update_fields": ["name", "job"]
        }
    ],
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "_api_name" : "elasticsearch_reader_api"
        },
        {
            "_op": "elasticsearch_bulk",
            "_api_name" : "elasticsearch_sender_api"
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
    "apis" : [
         {
            "_name": "elasticsearch_reader_api",
            "index": "test_index",
            "date_field_name": "created",
            "_connection": "es-1"
        },
        {
            "_name": "elasticsearch_sender_api",
            "index": "new_index",
            "type": "events",
            "upsert": true,
            "script": "ctx._source.count += add",
            "script_params": { "add": "add" }
        }
    ],
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "_api_name" : "elasticsearch_reader_api"
        },
        {
            "_op": "elasticsearch_bulk",
            "_api_name" : "elasticsearch_sender_api"
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

| Configuration | Description                                                   | Type   | Notes                                            |
| ------------- | ------------------------------------------------------------- | ------ | ------------------------------------------------ |
| \_op          | Name of operation, it must reflect the exact name of the file | String | required                                         |
| _api_name     | name of api to be used by elasticsearch bulk sender           | String | required, defaults to 'elasticsearch_sender_api' |
