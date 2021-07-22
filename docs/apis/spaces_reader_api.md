# spaces_reader_api

This is a [teraslice api](https://terascope.github.io/teraslice/docs/jobs/configuration#apis), which encapsulates a specific functionality that can be utilized by any processor, reader or slicer.

 The `spaces_reader_api` will provide an [api factory](https://terascope.github.io/teraslice/docs/packages/job-components/api/classes/apifactory), which is a singleton that can create, cache and manage multiple elasticsearch readers that can be accessed in any operation through the `getAPI` method on the operation.

This api is the core of the [spaces reader](../operations/spaces_reader.md). This contains all the same behavior, functionality and configuration of that reader

Fetched records will already have metadata associated with it, like the `_key` field. Please reference the [metadata section](#metadata) for more information.

## Usage
### Example Processor using a spaces reader API
This is an example of a custom fetcher using the spaces_reader_api to make its own queries to elasticsearch.
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
            "_op" : "my_reader",
            "api_name" : "spaces_reader_api"
        },
        {
            "_op" : "noop"
        }
    ]
}
```
Here is a custom fetcher for the job described above
```typescript
// found at my_reader/fetcher.ts
export default class MyReader extends Fetcher<ESDateConfig> {
    api!: elasticAPI.Client;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {});
    }

    async fetch(slice: ReaderSlice): Promise<DataEntity[]> {
        return this.api.fetch(query);
    }
}
```

## Spaces Reader Factory API Methods

### size

this will return how many separate reader apis are in the cache

### get
parameters:
- name: String

this will fetch any reader api that is associated with the name provided

### getConfig
parameters:
- name: String

this will fetch any reader api config that is associated with the name provided

### create (async)
parameters:
- name: String
- configOverrides: Check options below, optional

this will create an instance of a [reader api](#spaces_reader_instance), and cache it with the name given. Any config provided in the second argument will override what is specified in the apiConfig and cache it with the name provided. It will throw an error if you try creating another api with the same name parameter

### remove (async)
parameters:
- name: String

this will remove an instance of a reader api from the cache and will follow any cleanup code specified in the api code.

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
  _name: "spaces_reader_api",
  index: "test_index",
  date_field_name: "created",
  size: 1000,
  connection: "default",
  endpoint : "{ YOUR_ENDPOINT_HERE }",
  token : "{ YOUR_TOKEN_HERE }",
};


const apiManager = this.getAPI(apiName);

apiManager.size() === 0

// this will return an api cached at "normalClient" and it will use the default api config
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
  date_field_name: "created",
  size: 1000,
  connection: "other",
  endpoint : "{ YOUR_ENDPOINT_HERE }",
  token : "{ YOUR_TOKEN_HERE }",
}


await apiManger.remove('normalClient');

apiManager.size() === 1

apiManager.get('normalClient') === undefined

```

## Spaces Reader Instance
This is the reader class that is returned from the create method of the APIFactory. This returns a restricted [elastic-api](https://terascope.github.io/teraslice/docs/packages/elasticsearch-api/overview). Only the search and count methods will work appropriately.

### fetch
```(query: ElasticsearchSliceQuery) => Promise<DataEntities[]>```
This will perform an date range or wildcard query to elasticsearch and return the results of the query.

parameters:
- query: an slice query object
  -  start: string, must be paired with end to do a date range query.
  -  end: string, must be paired with start to do a date range query.
  -  wildcard: { field: string, value: string }, an elasticsearch wildcard query on string values. The value needs to be formatted in `key*`,please reference examples below.
  -  key: string, only used for _uid queries on elasticsearch v5 or older. The key need to be specified as `docType#key*` format, please reference examples below.


### count
```(query: ElasticsearchQuery) => Promise<number>```
This will perform an count query and return the number of records in that query.

parameters:
- query: an slice query object
  -  start: string, must be paired with end to do a date range query.
  -  end: string, must be paired with start to do a date range query.
  -  wildcard: { field: string, value: string }, an elasticsearch wildcard query on string values. The value needs to be formatted in `key*`,please reference examples below.
  -  key: string, only used for _uid queries on elasticsearch v5 or older. The key need to be specified as `docType#key*` format, please reference examples below.


parameters:
- query: an elasticsearch query object

### version
```() => number```
This returns the major elasticsearch version that this client is connected to

### verifyIndex
```() => Promise<void>```
This check if the index exists and throw otherwise, this will also log the window_size of that given index.


```js
const dateRangeQuery = {
    start: '2019-04-26T15:00:23.201Z',
    end: '2019-04-26T15:00:23.220Z',
};

const results = await api.fetch(dateRangeQuery);


const query: {
    q: '(test:query OR other:thing AND bytes:>=2000)',
    size: 100,
    fields: 'foo,bar,date'
};

const results = await api._searchRequest(query);

api.version === 6
```

## Parameters

| Configuration          | Description                                                                                                                                                                          | Type                                          | Notes                                                                                                                                                                      |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| \_name                 | Name of operation, it must reflect the exact name of the file                                                                                                                        | String                                        | required                                                                                                                                                                   |
| endpoint               | The base API endpoint to read from: i.e.http://yourdomain.com/api/v2                                                                                                                 | String                                        | required                                                                                                                                                                   |
| token                  | teraserver API access token for making requests                                                                                                                                      | String                                        | required                                                                                                                                                                   |
| timeout                | Time in milliseconds to wait for a connection to timeout                                                                                                                             | Number                                        | optional, defaults to 300000 ms or 5 mins                                                                                                                                  |
| index                  | Which index to read from                                                                                                                                                             | String                                        | required                                                                                                                                                                   |
| type                   | The type of the document that you are reading, used when a chuck is so large that it must be divided up by the documents \_id                                                        | String                                        | required if using elasticsearch v5, optional otherwise, defaults to '_doc'                                                                                                 |
| size                   | The limit to the number of docs pulled in a chunk, if the number of docs retrieved by the slicer exceeds this number, it will cause the slicer to recurse to provide a smaller batch | Number                                        | optional, defaults to 5000                                                                                                                                                 |
| start                  | The start date to which it will read from                                                                                                                                            | String/Number/ elasticsearch date math syntax | optional, inclusive , if not provided the index will be queried for earliest date, this date will be reflected in the opConfig saved in the execution context              |
| end                    | The end date to which it will read to                                                                                                                                                | String/Number/ elasticsearch date math syntax | optional, exclusive, if not provided the index will be queried for latest date, this date will be reflected in the opConfig saved in the execution context                 |
| interval               | The time interval in which the reader will increment by. The unit of time may be months, weeks, days, hours, minutes, seconds, milliseconds or their appropriate abbreviations       | String                                        | optional, defaults to auto which tries to calculate the interval by dividing date_range / (numOfRecords / size)                                                            |
| time_resolution        | Not all dates have millisecond resolutions, specify 's' if you need second level date slicing                                                                                        | String                                        | optional, defaults to milliseconds 'ms'                                                                                                                                    |
| date_field_name        | document field name where the date used for searching resides                                                                                                                        | String                                        | required                                                                                                                                                                   |
| query                  | specify any valid lucene query for elasticsearch to use in filtering                                                                                                                 | String                                        | optional                                                                                                                                                                   |
| fields                 | Used to restrict what is returned from elasticsearch. If used, only these fields on the documents are returned                                                                       | Array                                         | optional                                                                                                                                                                   |
| subslice_by_key        | determine if slice should be further divided up by id if slice is to too big                                                                                                         | Boolean                                       | optional, defaults to false                                                                                                                                                |
| subslice_key_threshold | used in determining when to slice a chunk by their \_ids                                                                                                                             | Number                                        | optional, defaults to 50000                                                                                                                                                |
| key_type               | Used to specify the key type of the \_ids of the documents being queried                                                                                                             | String                                        | optional, defaults to elasticsearch id generator (base64url)                                                                                                               |
| connection             | Name of the elasticsearch connection to use when sending data                                                                                                                        | String                                        | optional, defaults to the 'default' connection created for elasticsearch                                                                                                   |
| geo_field              | document field name where the geo data used for searching resides                                                                                                                    | String                                        | optional, is required if any geo parameter is set                                                                                                                          |
| geo_box_top_left       | used for a bounding box query                                                                                                                                                        | String/Geo Point                              | optional, must be paired with geo_box_bottom_right if used                                                                                                                 |
| geo_box_bottom_right   | used for a bounding box query                                                                                                                                                        | String/Geo Point                              | optional, must be paired with geo_box_top_left if used                                                                                                                     |
| geo_point              | used for a geo distance query                                                                                                                                                        | String/Geo Point                              | optional, must be paired with geo_distance if used                                                                                                                         |
| geo_distance           | used for a geo distance query (ie 200km)                                                                                                                                             | String                                        | optional, must be paired with geo_point if used.                                                                                                                           |
| geo_sort_point         | geo point for which sorting will be based on                                                                                                                                         | String/Geo Point                              | optional, is required for bounding box queries if any sort parameters are set. geo distance queries default to use geo_point as the sorting point if this value is not set |
| geo_sort_order         | the order used for sorting geo queries, can either be 'asc' or 'desc'                                                                                                                | String                                        | optional, defaults to 'asc'                                                                                                                                                |
| geo_sort_unit          | the unit of measurement for sorting, may be set to 'mi', 'km', 'm','yd', 'ft                                                                                                         | String                                        | optional, defaults to 'm'                                                                                                                                                  |
| variables              | can specify an xLuceneVariable object for the given xLucene query                                                                                                                    | Object                                        | optional                                                                                                                                                                   |

`NOTE`: a difference in behavior compared to the elasticsearch_reader is that the default geo distance sort will be ignored if any sort parameter is specified on the query. Sorting on geo distance while specifying another sorting parameter is still possible if you set any other geo sorting parameter, which will cause the query to sort by both.


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
