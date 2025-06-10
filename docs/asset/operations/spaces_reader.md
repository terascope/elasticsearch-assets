# spaces_reader #
The spaces_reader allows you to fetch data from a endpoint on a spaces server. This reader is a wrapper around the [elasticsearch_reader](./elasticsearch_reader.md) so it has all the same behaviors, functionality and requirements of that reader.

For this reader to function correctly the data must contain a date field with enough time resolution to slice the data into reasonably sized chunks and the data must be spread out through time. You must also have a valid space `endpoint` url and valid user `token` to communicate with the server.

Since this data is ordered, it can be sliced in parallel, and be read in parallel to have a very high throughput. If your index does not have a date field or for some reason the dates are all the same then this reader may not be able to process the data and you should consider using the [id_reader](./id_reader.md).

this is a [recoverable](https://terascope.github.io/teraslice/docs/management-apis/endpoints-json#post-v1jobsjobid_recover) reader, meaning that this job can be stopped, and then pick back up where it left off.

Fetched records will already have metadata associated with it, like the `_key` field. Please reference the [metadata section](#metadata) for more information.

## Usage

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
            "_op": "spaces_reader",
            "endpoint": "YOUR_ENDPOINT_HERE",
            "token": "YOUR_TOKEN_HERE",
            "index": "query_index",
            "field": "created",
            "query": "bytes:>=100",
            "start": "2020-08-04T10:00:00.000Z",
            "end": "2020-08-04T20:00:00.000Z"
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

## Parameters

| Configuration | Description | Type |  Notes   |
| --------- | -------- | ------ | ------ |
| \_op | Name of operation, it must reflect the exact name of the file | String | required |
| endpoint | The base API endpoint to read from: i.e. http://yourdomain.com/api/v1 | String | required |
| token | teraserver API access token for making requests | String | required |
| timeout | Time in milliseconds to wait for a connection to timeout | Number | optional, defaults to 300000 ms or 5 mins  |
| api_name | name of api to be used by spaces reader | String | optional, defaults to 'spaces_reader_api' |
| index | Which index to read from | String | required |
| size | The limit to the number of docs pulled in a chunk, if the number of docs retrieved by the slicer exceeds this number, it will cause the slicer to recurse to provide a smaller batch | Number | optional, defaults to 5000 |
| start | The start date to which it will read from | String/Number/ elasticsearch date math syntax | optional, inclusive , if not provided the index will be queried for earliest date, this date will be reflected in the opConfig saved in the execution context |
| end | The end date to which it will read to| String/Number/ elasticsearch date math syntax | optional, exclusive, if not provided the index will be queried for latest date, this date will be reflected in the opConfig saved in the execution context |
| interval | The time interval in which the reader will increment by. The unit of time may be months, weeks, days, hours, minutes, seconds, milliseconds or their appropriate abbreviations | String | optional, defaults to auto which tries to calculate the interval by dividing date_range / (numOfRecords / size) |
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
| caCertificate | CA certificate used to validate an https endpoint | String | optional |

`NOTE`: a difference in behavior compared to the elasticsearch_reader is that the default geo distance sort will be ignored if any sort parameter is specified on the query. Sorting on geo distance while specifying another sorting parameter is still possible if you set any other geo sorting parameter, which will cause the query to sort by both.

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
- `endpoint`
- `token`
- `timeout`
- `date_field_name`

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
            "_op" : "spaces_reader",
            "index" : "test_index",
            "endpoint" : "{ YOUR_ENDPOINT_HERE }",
            "token" : "{ YOUR_TOKEN_HERE }",
            "size" : 5000,
            "date_field_name" : "created"
        },
        {
            "_op" : "noop"
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
            "_name" : "spaces_reader_api",
            "index" : "test_index",
            "endpoint" : "{ YOUR_ENDPOINT_HERE }",
            "token" : "{ YOUR_TOKEN_HERE }",
            "size" : 5000,
            "date_field_name" : "created"
        }
    ],
    "operations" : [
        {
            "_op" : "spaces_reader",
            "api_name" : "spaces_reader_api"
        },
        {
            "_op" : "noop"
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
    _type:  "_doc",
    _index: "test_index",
    _version: undefined,
    _seq_no: undefined,
    _primary_term: undefined,
    _processTime: 1596663162372,
    _ingestTime: 1596663162372,
    _eventTime: 1596663162372,
}
```
