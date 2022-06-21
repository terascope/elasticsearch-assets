# elasticsearch_reader #

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
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "index": "test_index",
            "date_field_name": "created",
            "connection": "es-1"
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
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "index": "query_index",
            "date_field_name": "created",
            "query": "bytes:>=100",
            "start": "2020-08-04T10:00:00.000Z",
            "end": "2020-08-04T20:00:00.000Z",
            "connection": "es-1"
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
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "index": "test_index",
            "date_field_name": "created",
            "connection": "es-1"
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
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "index": "test_index",
            "date_field_name": "created",
            "delay": "1m",
            "interval": "1m",
            "connection": "es-1"
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
    "operations" : [
        {
            "_op": "elasticsearch_reader",
            "index": "test_index",
            "date_field_name": "created",
            "time_resolution": "ms",
            "interval": "1m",
            "connection": "es-1"
        },
        {
            "_op": "noop"
        }
    ]
}
```

## Parameters
| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| \_op| Name of operation, it must reflect the exact name of the file | String | required |
| index | Which index to read from | String | required |
| size | The limit to the number of docs pulled in a chunk, if the number of docs retrieved by the slicer exceeds this number, it will cause the slicer to recurse to provide a smaller batch | Number | optional, defaults to 5000 |
| start | The start date to which it will read from | String/Number/ elasticsearch date math syntax | optional, inclusive , if not provided the index will be queried for earliest date, this date will be reflected in the opConfig saved in the execution context |
| end | The end date to which it will read to| String/Number/ elasticsearch date math syntax | optional, exclusive, if not provided the index will be queried for latest date, this date will be reflected in the opConfig saved in the execution context |
| interval | The time interval in which the reader will increment by. The unit of time may be months, weeks, days, hours, minutes, seconds, milliseconds or their appropriate abbreviations | String | optional, defaults to auto which tries to calculate the interval by dividing date_range / (numOfRecords / size) |
| delay | Offset applied to reader of when to begin reading, must be in interval syntax e.g "5s" | String | Only used in persistent mode |
| time_resolution | Not all dates have millisecond resolutions, specify 's' if you need second level date slicing | String | optional, defaults to milliseconds 'ms' |
| date_field_name | document field name where the date used for searching resides | String | required |
| query | specify any valid lucene query for elasticsearch to use in filtering| String | optional |
| fields | Used to restrict what is returned from elasticsearch. If used, only these fields on the documents are returned | Array | optional |
| subslice_by_key | determine if slice should be further divided up by id if slice is to too big | Boolean | optional, defaults to false |
| subslice_key_threshold | used in determining when to slice a chunk by their \_ids | Number | optional, defaults to 50000 |
| key_type | Used to specify the key type of the \_ids of the documents being queried | String | optional, defaults to elasticsearch id generator (base64url) |
| connection | Name of the elasticsearch connection to use when sending data | String | optional, defaults to the 'default' connection created for elasticsearch |
| geo_field | document field name where the geo data used for searching resides | String | optional, is required if any geo parameter is set |
| geo_box_top_left | used for a bounding box query | String/Geo Point | optional, must be paired with geo_box_bottom_right if used |
| geo_box_bottom_right | used for a bounding box query | String/Geo Point | optional, must be paired with geo_box_top_left if used |
| geo_point | used for a geo distance query | String/Geo Point | optional, must be paired with geo_distance if used |
| geo_distance | used for a geo distance query (ie 200km) | String | optional, must be paired with geo_point if used. |
| geo_sort_point | geo point for which sorting will be based on | String/Geo Point | optional, is required for bounding box queries if any sort parameters are set. geo distance queries default to use geo_point as the sorting point if this value is not set |
| geo_sort_order | the order used for sorting geo queries, can either be 'asc' or 'desc' | String | optional, defaults to 'asc' |
| geo_sort_unit | the unit of measurement for sorting, may be set to 'mi', 'km', 'm','yd', 'ft | String | optional, defaults to 'm' |


- as the query parameter expects a lucene query (which does not support geo queries), the geo parameters provide additional ways to filter records and will be used in conjunction with query

- Geo points are written in the format of: '33.4484,112.0740' , which is 'latitude,longitude'

- start and end may be specified in elasticsearch's [date math syntax](https://www.elastic.co/guide/en/elasticsearch/reference/2.x/common-options.html#date-math)

- for geo distance queries, it defaults to sorting the returning results based off either the geo_point, or the geo_sort_point if specified. The results from a bounding box queriers are not sorted by default.


## Advanced Configuration

#### interval
by default, interval is set to auto. This will tell the reader to to make a calculation with the date range, count of the range and the `size` parameter to determine an `interval` value. This works great in most circumstances but this assumes a semi-evenly distributed data across the time range.

If the data is sparse, or heavily lopsided (meaning the range is large, but most dates live in a certain part of the range) then the auto interval may be inappropriate. It could be making a lot of small 5s slices when it needs to jump a week in time. In this case it might be better to set a larger interval to make the jumps and allow it to recurse down when it needs to.

Its a balancing act, and you need to know your data. An interval too small will make spam the elasticsearch cluster with many requests, especially if the count is small for each small slice. However having it to big will have cost as it will then need to split the segment of time and query again to see if that new time segment is digestible.

#### subslice_by_key
When you have a very large slice that cannot be further broken up by time, as in there are 500k records all in the same time (as determined by `time_resolution` config) this will try to further divide the dates by using the `id_reader` on a given key. However, its usually a better idea to use the id_reader in the first place if you get to that point, but this allows an escape hatch. Use at your own risk.

#### Note on common errors ####
- You must be aware of how your dates are saved in elasticsearch in a given index. If you specify your start or end dates as common '2016-01-23' dates, it is likely the reader will not reach data that have dates set in utc as the time zone difference may set it in the next day. If you would like to go through the entire index, then leave start and end empty, the job will find the dates for you and later be reflected in the execution context (ex) configuration for this operation

- If you are using elasticsearch >= 2.1.0 they introduced a default query limit of 10000 docs for each index which will throw an error if you query anything above that. This will pose an issue if you set the size to big or if you have more than 10000 docs within 1 millisecond, which is the shortest interval the slicer will attempt to make before overriding your size setting for that slice. Your best option is to raise the max_result_window setting for that given index.

- this reader assumes linear date times, and this slicer will stop at the end date specified or the end date determined at the starting point of the job. This means that if an index continually grows while this is running, this will not reach the new data, you would to start another job with the end date from the other job listed as the start date for the new job


#### API usage in a job
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
            "date_field_name": "created",
            "size": 1000,
            "connection": "default"
        },
        {
            "_op": "noop"
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
            "date_field_name": "uuid",
            "size": 1000,
            "connection": "default"
        }
    ],
    "operations" : [
        {
            "_op" : "elasticsearch_reader",
            "api_name" : "elasticsearch_reader_api"
        },
        {
            "_op": "noop"
        }
    ]
}
```

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
