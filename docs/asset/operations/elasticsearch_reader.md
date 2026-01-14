# elasticsearch_reader

The elasticsearch_reader reads data from an Elasticsearch index using an algorithm that partitions the data using date range queries. This enables high throughput reading of data out of very large indices without having to use deep paging or state dependent scan/scroll.

For this reader to function correctly the data must contain a date field with enough time resolution to slice the data into reasonably sized chunks and the data must be spread out through time. The exact time resolution required depends on the overall size of the index. If you have a small amount of data hourly or daily may be adequate but if the index contains 100 billion records you may need times down to the millisecond level in order to slice it small enough.

Since this data is ordered, it can be sliced in parallel, and be read in parallel to have a very high throughput. If your index does not have a date field or for some reason the dates are all the same then this reader may not be able to process the data and you should consider using the [id_reader](./id_reader.md).

this is a [recoverable](https://terascope.github.io/teraslice/docs/management-apis/endpoints-json#post-v1jobsjobid_recover) reader, meaning that this job can be stopped, and then pick back up where it left off.

Fetched records will already have metadata associated with it, like the `_key` field. Please reference the [metadata section](#metadata) for more information.

## Usage

### Batch read the entire content of an index

Here is an example of a job that will check the `test_index` index and query against the `created` field for date values. Since no `start` or `end` is specified it will read the entire index. This will use the `es-1` connection configuration described in your [terafoundation connector config](https://terascope.github.io/teraslice/docs/configuration/overview#terafoundation-connectors)

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
        }
    ],
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "_api_name" : "elasticsearch_reader_api"
        },
        {
            "_op": "noop"
        }
    ]
}
```

Here is an example of elasticsearch data being fetched and returned from the reader in order

```javascript
const elasticsearchIndexData = [
    { created: "2020-08-04T16:38:18", id: 1 },
    { created: "2020-08-04T16:38:20", id: 2 },
    { created: "2020-08-04T20:38:19", id: 3 },
    { created: "2020-08-04T16:38:19", id: 4 }
]

// will be fetched and ordered from earliest to latest

const expected fetchResults = [
    { created: "2020-08-04T16:38:18", id: 1 },
    { created: "2020-08-04T16:38:19", id: 4 },
    { created: "2020-08-04T16:38:20", id: 2 },
    { created: "2020-08-04T20:38:19", id: 3 },
]
```

`Notes`: since this job has one worker and slicer, the data fetched is sequential
and that worker will process all the data in order.

### Batch read a filtered subset of an index

Here is an example of of a job that will check the `query_index` and get records between start date(inclusive) and the `end` date (exclusive). This will also filter based on the `query` parameter. This will use the `es-1` connection configuration described in your [terafoundation connector config](https://terascope.github.io/teraslice/docs/configuration/overview#terafoundation-connectors)

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
            "index": "query_index",
            "date_field_name": "created",
            "query": "bytes:>=100",
            "start": "2020-08-04T10:00:00.000Z",
            "end": "2020-08-04T20:00:00.000Z",
            "_connection": "es-1"
        }
    ],
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "_api_name" : "elasticsearch_reader_api"
        },
        {
            "_op": "noop"
        }
    ]
}
```

Here is a representation of what data is being returned from the additional lucene query along with the date range restrictions

```javascript
const elasticsearchIndexData = [
    { created: "2020-08-04T16:38:18.372Z", id: 1, bytes: 213 },
    { created: "2020-08-04T17:18:20.582Z", id: 2, bytes: 13 },
    { created: "2020-08-04T20:38:19.132Z", id: 3, bytes: 2455 },
    { created: "2020-08-04T20:00:00.745Z", id: 4, bytes: 453 }
]

// will be fetched and ordered from earliest to latest

const expected fetchResults = [
    { created: "2020-08-04T16:38:18.372Z", id: 1, bytes: 213 },
]
```

`Notes`: be careful to take note of the `start` and `end` dates you specify, especially the formatting (UTC vs ISO etc) compared to the data in elasticsearch. Make sure to use the same formatting so you can filter the data you want. If one date encapsulates time zones but the other doesn't, you could be several hours off and get the wrong results.

### Higher Throughput Job

This job has 4 slicers, it will determine the date range for the entire index and divide it by four which will be the date range for each slicer. Each slicer will determine processable slice chunks from earliest to latest within their assigned ranges. The slices will be doled out to the 35 workers as they come in so a given worker make process later dates, then on the next slice process early dates. Some slicers may finish earlier than others.

Example Job

```json
{
    "name" : "testing",
    "workers" : 35,
    "slicers" : 4,
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
        }
    ],
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "_api_name" : "elasticsearch_reader_api"
        },
        {
            "_op": "noop"
        }
    ]
}
```

### Persistent Job

When lifecycle is set to persistent, this will try reading from a stream of input. When the execution starts, it will try to read within the range of the `interval` with a latency time specified in `delay`

Example Job

```json
{
    "name" : "testing",
    "workers" : 5,
    "slicers" : 1,
    "lifecycle" : "persistent",
    "assets" : [
        "elasticsearch"
    ],
    "apis" : [
        {
            "_name": "elasticsearch_reader_api",
            "index": "test_index",
            "date_field_name": "created",
            "delay": "1m",
            "interval": "1m",
            "_connection": "es-1"
        }
    ],
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "_api_name" : "elasticsearch_reader_api"
        },
        {
            "_op": "noop"
        }
    ]
}
```

Here is a representation of when a job starts, how the internal clock of the reader is calculated with the interval and delay parameters

```javascript
// this is the server start time of the job
const currentExecutionTime = "2020-08-04T16:30:00"

// with the minute delay and the interval of 1m, this is where we start
const startRange = "2020-08-04T14:30:00";
const endRange = "2020-08-04T15:30:00";

// after 1 min passes in server time, the end is increased by the interval
const currentExecutionTime = "2020-08-04T17:30:00"

// We are always behind the current time - delay
const endRange = "2020-08-04T16:30:00";
```

### Hinting the algorithm to optimize performance

By default the reader will do its best to quickly slice the data down to a reasonable size however sometimes really large jobs can use a few hints to run more efficiently. If the record's date allow, changing the `interval` and `time_resolution` allows more fine grained control.

The example job will try to read in 1 minute chunks, and if it needs make smaller chunks, it can chunk at the millisecond resolution.

Example Job

```json
{
    "name" : "testing",
    "workers" : 5,
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
            "time_resolution": "ms",
            "interval": "1m",
            "_connection": "es-1"
        }
    ],
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "_api_name" : "elasticsearch_reader_api"
        },
        {
            "_op": "noop"
        }
    ]
}
```

## Parameters

| Configuration | Description                                                   | Type   | Notes    |
| ------------- | ------------------------------------------------------------- | ------ | -------- |
| _op          | Name of operation, it must reflect the exact name of the file | String | required |
| _api_name     | Name of api used for s3_reader                                | String | required |



In elasticsearch_assets v5, teraslice apis must be set within the job configuration. Teraslice will no longer automatically setup the api for you. All fields related to the api that were previously allowed on the operation config must be specified in the api config. Configurations for the api should no longer be set on the operation as they will be ignored. The api's `_name` must match the operation's `_api_name`.
