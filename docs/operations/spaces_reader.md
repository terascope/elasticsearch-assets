# spaces_reader #
This is a wrapper around the elasticsearch_reader so it has all of the functionality, schemas and validations of that reader. This is used to allow client access to data through communication with a spaces server.

| Configuration | Description | Type |  Notes   |
| --------- | -------- | ------ | ------ |
| \_op | Name of operation, it must reflect the exact name of the file | String | required |
| endpoint | The base API endpoint to read from: i.e.http://yourdomain.com/api/v1 | String | required |
| token | teraserver API access token for making requests | String | required |
| timeout | Time in milliseconds to wait for a connection to timeout | Number | optional, defaults to 300000 ms or 5 mins  |
| api_name | name of api to be used by spaces reader | String | optional, defaults to 'spaces_reader_api' |
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


## Example Job

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
            "endpoint" : "http://someurl.com/api/v1",
            "token" : "as98d7fhkjqwekjh123897asdfl",
            "size" : 5000,
            "date_field_name" : "created"
        },
        {
            "_op" : "noop"
        }
    ],
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
            "endpoint" : "http://someurl.com/api/v1",
            "token" : "as98d7fhkjqwekjh123897asdfl",
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
    ],
}
```

`NOTE`: If start with the long form (which means to set up the apis manually and specify the api_name on the operation) then there are restrictions on what configurations you can put on the operation so that clashing of configurations are minimalized. The api configs take precendence.

If submitting the job in long form, here is a list of parameters that will throw an error if also specified on the opConfig, since these values should be placed on the api:
- `index`
- `endpoint`
- `token`
- `timeout`
- `date_field_name`
