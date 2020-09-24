# elasticsearch_reader_api

The `elasticsearch_reader_api` makes the elasticsearch reader functionality available to any processor, reader or slicer.   It's a [teraslice api](https://terascope.github.io/teraslice/docs/jobs/configuration#apis) that uses the [api factory](https://terascope.github.io/teraslice/docs/packages/job-components/api/classes/apifactory) to create, cache, and manage multiple elasticsearch readers.   This api is the core of the [elasticsearch reader](../operations/elasticsearch_reader.md) and [id reader](../operations/id_reader.md) operations.
The records fetched via this api will have the standard associated metadata fields, e.g., `_key`, `_process_time`,`_ingest_time`, etc... See the [metadata section](#metadata) for details about metadata fields.

## Usage
### Example Processor using the elasticsearch reader API
Custom fetcher

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
            "field": "uuid",
            "size": 1000,
            "connection": "default"
        }
    ],
    "operations" : [
        {
            "_op" : "some_reader",
            "api_name" : "elasticsearch_reader_api"
        },
        {
            "_op": "noop"
        }
    ]
}
```

Custom fetcher code
```js
// found at  /some_reader/fetcher.js
export default class SomeReader extends Fetcher {
    async initialize() {
        await super.initialize();
        const apiManager = this.getAPI(this.opConfig.api_name;);
        this.api = await apiManager.create(apiName, {});
    }

    async fetch(slice) {
        return this.api.fetch(slice);
    }
}
```

## Elasticsearch Reader Factory API Methods

### size

Returns the number of reader apis in the cache

### get
parameters:
- name: String

Returns the reader api associated with the name

### getConfig
parameters:
- name: String

Returns the reader api config associated with the name

### create (async)
parameters:
- name: String
- configOverrides (optional) see [parameters](#Parameters) for config fields

Creates an instance of a [reader api](#elasticsearch-reader-instance). Any config provided in the second argument will override what is specified in the apiConfig. Throws an error if the api name has been previously used.  

### remove (async)
parameters:
- name: String

Removes an instance of a reader api from the cache and will follow any cleanup specified in the api code.

### entries

Returns a Map of objects, `{ api_name: api_instance }`, of the cached names and api instances.

### keys

Returns a Map of the names of the cached api names

### values

Returns a Map of the cached api instances

## Example of using the factory api methods
```javascript
// example of api configuration
const apiConfig = {
  _name: "elasticsearch_reader_api",
  index: "test_index",
  field: "uuid",
  size: 1000,
  connection: "default"
};

const apiManager = this.getAPI(apiName);

apiManager.size() === 0

// this will return an api cached at "normalClient" 
const normalClient = await apiManager.create('normalClient', {})

apiManager.size() === 1

apiManager.get('normalClient') === normalClient

// this will return an api cached at "overrideClient" and it will use the api config but override the index to "other_index" in the new instance.
const overrideClient = await apiManager.create('overrideClient', { index: 'other_index', connection: "other" })

apiManager.size() === 2

// this will return the full configuration for this client
apiManger.getConfig('overrideClient') === {
  _name: "elasticsearch_reader_api",
  index: "other_index",
  field: "uuid",
  size: 1000,
  connection: "other"
}

// iterate through all the cached api names
for (const keys of this.apiManager.keys()) {
    console.log(keys); // normalClient then overrideClient
}

await apiManger.remove('normalClient');

apiManager.size() === 1

apiManager.get('normalClient') === undefined
```

## Elasticsearch Reader Instance
This is the reader api instance that is returned from the create method of the APIFactory.  The api methods can then be used to fetch and query data from elasticsearch.

Create a new reader instance:
```js
const api = apiManager.create('newApi', {});
```

Get a previously created reader instance:
```js
const api = apiManager.get('oldApi');
```

### fetch (async)

Returns the results of a date range or wildcard query

parameters:
- query: an slice query object
  -  start: string, must be paired with end to do a date range query.
  -  end: string, must be paired with start to do a date range query.
  -  wildcard: { field: string, value: string }, an elasticsearch wildcard query on string values. The value needs to be formatted in `key*`,please reference examples below.
  -  key: string, only used for _uid queries on elasticsearch v5 or older. The key need to be specified as `docType#key*` format, please reference examples below.

```js
const api = apiManager.create('newApi', {});

const dateRangeQuery = {
    start: '2019-04-26T15:00:23.201Z',
    end: '2019-04-26T15:20:23.220Z',
};

let results = await api.fetch(dateRangeQuery);
results === [ { some: 'record', created: '2019-04-26T15:10:23.220Z' }]

const oldUIDQuery = {
   key:  'events#ba*'
};

let results = await api.fetch(oldUIDQuery);
results === [ { some: 'record', created: '2019-04-26T15:10:23.220Z' }]

const wildcardQuery = {
    field: 'uuid',
    value: 'afe1*',
};

let results = await api.fetch(wildcardQuery);
results === [ { some: 'record', uuid: 'afe18550-0081-453f-9e80-93a90782a5bd' }]
```


### count (async)

Returns the number of records that are in the range of the query.

parameters:
- query: a slice query object
  -  start: string, must be paired with end to do a date range query.
  -  end: string, must be paired with start to do a date range query.
  -  wildcard: { field: string, value: string }, an elasticsearch wildcard query on string values. The value needs to be formatted in `key*`,please reference examples below.
  -  key: string, only used for _uid queries on elasticsearch v5 or older. The key need to be specified as `docType#key*` format, please reference examples below.

```js
const api = apiManager.create('countApi', {});

const dateRangeQuery = {
    start: '2019-04-26T15:00:23.201Z',
    end: '2019-04-26T15:20:23.220Z',
};

let results = await api.count(dateRangeQuery);
results === 122

const oldUIDQuery = {
   key:  'events#ba*'
};

let results = await api.count(oldUIDQuery);
results === 3552

const wildcardQuery = {
    field: 'uuid',
    value: 'afe1*',
};

let results = await api.count(wildcardQuery);
results === 1232
```

### _searchRequest (async)

Returns results from a custom elasticsearch query

`WARNING: _searchRequest is an internal api and likely to change.`

parameters:
- query: an elasticsearch query object

```js
const query: {
    index: 'example-index',
    q: '(test:query OR other:thing AND bytes:>=2000)',
    size: 100
};

const results = await api._searchRequest(query);
```

### version

Returns the major elasticsearch version that this client is connected to

```js
api.version === 6
```

### verifyIndex (async)

Verifies that the index exists or throws an error if it is not found.  It also logs the window_size of the index.

```js
try {
    await api.verifyIndex()
} catch(err) {
    // handle error or create index because it does not exists or is not available
}
```

### determineSliceInterval (async)

A helper api used to determine the size of the slice interval for the date_slicer.  If interval is set to `auto`, dateRange must be provided and the function will calculate an interval. If passed in a duration, it will parse the duration in the format listed by the `time_resolution` configuration.

parameters:
- interval: a duration string (ie. `5min`, `30s`, `750ms`), or it may be set to `auto`
- dateRange: optional, only used when interval is set to `auto`,
  - start: a start date
  - limit: the end date


```js
const apiConfig = {
    time_resolution: 'ms'
}

const interval = await api.determineSliceInterval('2s');
interval === [2000, 'ms'];


const dateRange = {
    start: '2019-04-26T15:00:23.201Z',
    limit: '2019-04-26T15:20:23.220Z',
};

const interval = await api.determineSliceInterval('auto', dateRange);
interval === [2763533, 'ms'];
```

### makeDateSlicer (async)

Generates a slicer based on the elasticsearch_reader slicer.

parameters:
- args: an slice query object
  -  lifecycle: must be set to `once` or `persistent`.
  -  slicerID: the numerical id of the slicer.
  -  numOfSlicers: the number of total slicers that will be generated
  -  recoveryData?: the `SlicerRecoveryData` for the job
  -  windowState?: the windowState controller, *only needed in persistent mode*
  -  startTime?: the start time of the operation, *only needed in persistent mode*
  -  hook?: an optional async callback that you can provide to handle the updates that the slicer creates (start/end times of the index, the interval for the job etc)

Once mode configuration
```js
const lifecycle = 'once';
const numOfSlicers = 3;
const slicerID = 0;
const recoveryData = [];

const hook = async (params) => {
    // params: { interval: [number, "time unit"], start: Date, end: Date}
  console.log(`Final job configuration ${JSON.stringify(params)}`)
};

const slicerConfig: DateSlicerArgs = {
   lifecycle,
   numOfSlicers,
   slicerID,
   recoveryData,
   hook,
};

const slicer = await this.api.makeDateSlicer(slicerConfig);

const results = await slicer();
results === {
    start: '2019-04-26T15:00:23.201Z',
    end: '2019-04-26T16:00:23.201Z',
    limit: '2019-04-26T15:20:23.220Z',
    count: 3467
}
```

Persistent mode configuration
```js
const lifecycle = 'persistent';
const numOfSlicers = 3;
const slicerID = 0;
const recoveryData = [];
const startTime = new Date().toISOString();
const windowState = this.api.makeWindowState(slicers);

const slicerConfig: DateSlicerArgs = {
   lifecycle,
   numOfSlicers,
   slicerID,
   recoveryData,
   startTime,
   windowState
};

const slicer = await this.api.makeDateSlicer(slicerConfig);

const results = await slicer();

results === {
    start: '2019-04-26T15:00:23.201Z',
    end: '2019-04-26T16:00:23.201Z',
    limit: '2019-04-26T15:20:23.220Z',
    count: 3467
}
```

### makeWindowState (async)

A helper method that will return a synchronization window to coordinate slicer date range progression. THIS IS ONLY NEEDED FOR A DATE SLICER IN *PERSISTENT* MODE. This needs to be made once at the top level of the processor/slicer and passed in each time `makeDateSlicer` is called.
See [makeDateSlicer](#makeDateSlicer) example above.

```js
const windowState = await api.makeWindowState();
```

### makeIDSlicer (async)

Generates a slicer based on the id_reader slicer.

parameters:
- args: an slice query object
  -  lifecycle: must be set to `once` or `persistent`.
  -  slicerID: the numerical id of the slicer.
  -  numOfSlicers: the number of total slicers that will be generated
  -  recoveryData?: the `SlicerRecoveryData` for the job
  -  key_type: The type of id used in index or the chars contained in the field'
  -  key_range?: if provided, slicer will only recurse on these given keys
  -  starting_key_depth: the length of the key generated before attempting to count the records in the index


```js
const lifecycle = 'once';
const numOfSlicers = 3;
const slicerID = 0;
const recoveryData = [];
const key_type = 'hexadecimal';
const starting_key_depth = 2;

const args = {
    lifecycle,
    numOfSlicers,
    slicerID,
    recoveryData,
    key_type,
    starting_key_depth,
};

const slicer = await api.makeIDSlicer(args);

const results = await slicer();
results ===  { key: 'a0*', count: 5 }
```
### determineDateRanges (async)

A helper method used to parse the start/end dates set on the apiConfig. If no start or end is specified it will query against the index to find the earliest and latest record, and produce dates to include them.

```js

const apiConfig = {
    start: '2019-04-26T15:00:23.201Z',
    index: 'some_index'
}

const results = await api.determineDateRanges()

results ==== {
    start: moment('2019-04-26T15:00:23.201Z'),
    limit: moment('2019-04-26T15:20:23.220Z'),
}

```

### getWindowSize

A helper method used to get the `index.max_result_window` size setting configured on the index. This is useful to determine how large a slice is permitted.

```js
const size = await api.getWindowSize();

size === 100000
```

## Parameters

| Configuration | Description | Type |  Notes   |
| --------- | -------- | ------ | ------ |
| \_name | Name of the api being used | String | required |
| timeout | Time in milliseconds to wait for a connection to timeout | Number | optional, defaults to 300000 ms or 5 mins  |
| index | Which index to read from | String | required |
| type | The type of the document that you are reading, used when a chuck is so large that it must be divided up by the documents \_id|String | required if using elasticsearch v5 and subslice_by_key is set to true, optional otherwise, defaults to '_doc' |
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

`NOTE`: a difference in behavior compared to the elasticsearch_reader is that the default geo distance sort will be ignored if any sort parameter is specified on the query. Sorting on geo distance while specifying another sorting parameter is still possible if you set any other geo sorting parameter, which will cause the query to sort by both.

### Example on using multiple elasticsearch_reader_api settings in the same job

 You can specify which api config applies to which _op by appending a semi-colon and an id to the end of the api `_name`.

 example job:
```json
{
    "name": "test-job",
    "lifecycle": "once",
    "workers": 1,
    "assets": ["elasticsearch"],
    "apis": [
        {
            "_name": "elasticsearch_reader_api:id",
            "connection": "connection-1",
            "index": "index-1",
            "field": "_key",
            "query": "key:key-name",
            "size": 10000
        },
        {
            "_name": "elasticsearch_reader_api:custom",
            "connection": "connection-2",
            "index": "index2",
            "field": "name",
            "size": 10000
        }
    ],
    "operations": [
        {
            "_op": "id_reader",
            "api_name": "elasticsearch_reader_api:id"
        },
        {
            "_op": "custom-api-reader-op",
            "api_name": "elasticsearch_reader_api:custom"
        }
    ]
}
```

Processor for the custom-api-reader-op
```js
'use strict';

const { BatchProcessor } = require('@terascope/job-components');

class CustomAPIReaderOp extends BatchProcessor {
    async initialize() {
        this.apiManager = this.getAPI(this.opConfig.api_name);
        this.api = await this.apiManager.create('customClient', {});

        // _searchRequest needs an index as part of the elasticsearch object query
        this.index = this.apiManager.getConfig('customClient').index;
    }

    async onBatch(data) {
        // function that builds a query from the id_readers output
        const query = this._buildQuery(data);

        const results = await this.newClient._searchRequest({
            q: query,
            index: this.index,
            size: 1000
        });

        this.apiManager.remove('customClient');

        return results;
    }
}

module.exports = CustomAPIReaderOp;
```

### Metadata
The metadata fields are calculated  by teraslice or provided by elasticsearch and attached to each record fetched from elasticsearch.  The metadata fields can then be called or referenced during a job by using the `getMetadata` method.

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
    "_type" : "events",
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
    _type:  "events",
    _index: "test_index",
    _version: undefined,
    _seq_no: undefined,
    _primary_term: undefined,
    _processTime: 1596663162372,
    _ingestTime: 1596663162372,
    _eventTime: 1596663162372,
}
```
