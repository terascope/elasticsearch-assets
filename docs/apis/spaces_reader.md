# spaces_reader_api

The `spaces_reader_api` will provide a factory that can create file reader apis that can be accessed in any operation through the `getAPI` method on the operation.


This is a [Factory API](https://terascope.github.io/teraslice/docs/packages/job-components/api/interfaces/apifactoryregistry), which can be used to fully manage api creation and configuration.


## Spaces Reader Factory API Methods

### size

this will return how many seperate reader apis are in the cache

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

this will create an instance of a reader api, and cache it with the name given. Any
config provided in the second argument will override what is specified in the apiConfig and cache it with the name provided. It will throw an error if you try creating another api with the same name parameter

```typescript
    const apiManager = this.getAPI<FileReaderFactoryAPI>(apiName);
    // this will return an api cached at "normalClient" and this instance will use all configurations listed on the apiConfig
    const client = apiManager.create('normalClient')

    // this will return an api cached at "overrideClient" and this instance will have an override setting the parameter compression to "gzip", this will use the rest of the configurations listed in the apiConfig
    const overrideClient = apiManager.create('overrideClient', { compression: 'gzip'})
```

### remove (async)
parameters:
- name: String

this will remove an instance of a reader api from the cache and will follow any cleanup code specified in the api code.

### entries

This will allow you to iterate over the cache name and client of the cache

### keys

This will allow you to iterate over the cache name of the cache

### values

This will allow you to iterate over the clients of the cache


## Spaces Reader Instance
This is the reader class that is returned from the create method of the APIFactory

### search
```(query: ElasticsearchQuery) => Promise<ESSearchResults>```
parameters:
- query: an elasticsearch query object

### search
```(query: ElasticsearchQuery) => Promise<ESSearchResults>```
parameters:
- query: an elasticsearch query object

This method will send the records to file

```js
    // this will read the first 500 bytes of the file
    const slice = {
        path: 'some/data/path',
        total: 10000,
        length: 500,
        offset: 0
    }
    const results = await api.read(docs)
```

## Options

| Configuration | Description | Type |  Notes   |
| --------- | -------- | ------ | ------ |
| \_name | Name of operation, it must reflect the exact name of the file | String | required |
| endpoint | The base API endpoint to read from: i.e.http://yourdomain.com/api/v1 | String | required |
| token | teraserver API access token for making requests | String | required |
| timeout | Time in milliseconds to wait for a connection to timeout | Number | optional, defaults to 300000 ms or 5 mins  |
| index | Which index to read from | String | required |
| type | The type of the document that you are reading, used when a chuck is so large that it must be divided up by the documents \_id|String | required if using elasticsearch v5, optional otherwise, defaults to '_doc' |
| size | The limit to the number of docs pulled in a chunk, if the number of docs retrieved by the slicer exceeds this number, it will cause the slicer to recurse to provide a smaller batch | Number | optional, defaults to 5000 |
| start | The start date to which it will read from | String/Number/ elasticsearch date math syntax | optional, inclusive , if not provided the index will be queried for earliest date, this date will be reflected in the opConfig saved in the execution context |
| end | The end date to which it will read to| String/Number/ elasticsearch date math syntax | optional, exclusive, if not provided the index will be queried for latest date, this date will be reflected in the opConfig saved in the execution context |
| interval | The time interval in which the reader will increment by. The unit of time may be months, weeks, days, hours, minutes, seconds, millesconds or their appropriate abbreviations | String | optional, defaults to auto which tries to calculate the interval by dividing date_range / (numOfRecords / size) |
| time_resolution | Not all dates have millisecond resolutions, specify 's' if you need second level date slicing | String | optional, defaults to milliseconds 'ms' |
| date_field_name | document field name where the date used for searching resides | String | required |
| query | specify any valid lucene query for elasticsearch to use in filtering| String | optional |
| fields | Used to restrict what is returned from elasticsearch. If used, only these fields on the documents are returned | Array | optional |
| subslice_by_key | determine if slice should be further divided up by id if slice is to too big | Boolean | optional, defaults to false |
| subslice_key_threshold | used in determining when to slice a chunk by thier \_ids | Number | optional, defaults to 50000 |
| key_type | Used to specify the key type of the \_ids of the documents being queryed | String | optional, defaults to elasticsearch id generator (base64url) |
| connection | Name of the elasticsearch connection to use when sending data | String | optional, defaults to the 'default' connection created for elasticsearch |
| geo_field | document field name where the geo data used for searching resides | String | optional, is required if any geo parameter is set |
| geo_box_top_left | used for a bounding box query | String/Geo Point | optional, must be paired with geo_box_bottom_right if used |
| geo_box_bottom_right | used for a bounding box query | String/Geo Point | optional, must be paired with geo_box_top_left if used |
| geo_point | used for a geo distance query | String/Geo Point | optional, must be paired with geo_distance if used |
| geo_distance | used for a geo distance query (ie 200km) | String | optional, must be paired with geo_point if used. |
| geo_sort_point | geo point for which sorting will be based on | String/Geo Point | optional, is required for bounding box queries if any sort parameters are set. geo distance queries default to use geo_point as the sorting point if this value is not set |
| geo_sort_order | the order used for sorting geo queries, can either be 'asc' or 'desc' | String | optional, defaults to 'asc' |
| geo_sort_unit | the unit of measurement for sorting, may be set to 'mi', 'km', 'm','yd', 'ft | String | optional, defaults to 'm' |

`NOTE`: a difference in behavior compared to the elasticsearch_reader is that the default geo distance sort will be ignored if any sort paramter is specified on the query. Sorting on geo distance while specifiying another sorting parameter is still possible if you set any other geo sorting parameter, which will cause the query to sort by both.


### Example Processor using a file reader api
```typescript
export default class SomeFetcher extends Fetcher<SomeConfig> {
    api!: S3Reader

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<FilereaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName);
    }

    async fetch(slice: SlicedFileResults): Promise<DataEntity[]> {
        // can do anything with the slice before reading
        return this.api.read(slice);
    }
}
```
