# elasticsearch_reader #
Used to retrieve elasticsearch data based on dates. This reader has different behaviour if lifecycle is set to "once" or "persistent"

Example configuration if lifecycle is set to "once"


In this mode, there is a definite start (inclusive) and end time (exclusive). Each slice will be based off of the interval and size configurations.
If the number of documents exceed the size within a given interval, it will recurse and and split the interval in half continually until the number of documents is less than or equal to size. If this cannot be achieved then the size of the chunk will be calculated against a threshold , and if it passes the threshold it further subdivides the range by the documents \_id's, else the slicer will ignore the size limit and process the chunk as is.


| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| \_op| Name of operation, it must reflect the exact name of the file | String | required |
| index | Which index to read from | String | required |
| type | The type of the document that you are reading, used when a chuck is so large that it must be divided up by the documents \_id|String | required |
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


Geo points are written in the format of: '33.4484,112.0740' , which is 'latitude,longitutde'

start and end may be specified in elasticsearch's [date math syntax](https://www.elastic.co/guide/en/elasticsearch/reference/2.x/common-options.html#date-math)

Note: for geo distance queries, it defaults to sorting the returning results based off either the geo_point, or the geo_sort_point if specified. The results from a bounding box querires are not sorted by default.

#### persistent mode ####

Example configuration if lifecycle is set to "persistent"

```json
{
    "_op": "elasticsearch_reader",
    "index": "someindex",
    "size": 5000,
    "interval": "5s",
    "delay": "1m",
    "date_field_name": "created"
}
```

The persistent mode expects that there is a continuous stream of data coming into elasticsearch and that it has a date field when it was uploaded. On initializing this job, it will begin reading at the current date (new Date()) minus the delay. The reader will then begin processing at the interval chunk you specify, and will read the next interval after the interval time has passed.

 E.g. using the job listed above and current time is "2016-01-27T13:48:05-07:00", it will attempt to start reading at start:"2016-01-27T13:47:00-07:00", end: "2016-01-27T13:47:05-07:00". After 5s has passed it will read start:"2016-01-27T13:47:05-07:00", end: "2016-01-27T13:47:10-07:00" thus keeping the 1m delay.

 The delay mechanism allows you to adjust for your elasticsearch refresh rate, network latency so that it can provide ample time to ensure that your data has been flushed.

##### Differences #####
No start or end keys

| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| delay | Offset applied to reader of when to begin reading, must be in interval syntax e.g "5s" | String | required |

##### Note on common errors #####
- You must be aware of how your dates are saved in elasticsearch in a given index. If you specify your start or end dates as common '2016-01-23' dates, it is likely the reader will not reach data that have dates set in utc as the time zone difference may set it in the next day. If you would like to go through the entire index, then leave start and end empty, the job will find the dates for you and later be reflected in the execution context (ex) configuration for this operation

- If you are using elasticsearch >= 2.1.0 they introduced a default query limit of 10000 docs for each index which will throw an error if you query anything above that. This will pose an issue if you set the size to big or if you have more than 10000 docs within 1 millisecond, which is the shortest interval the slicer will attempt to make before overriding your size setting for that slice. Your best option is to raise the max_result_window setting for that given index.

- this reader assumes linear date times, and this slicer will stop at the end date specified or the end date determined at the starting point of the job. This means that if an index continually grows while this is running, this will not reach the new data, you would to start another job with the end date from the other job listed as the start date for the new job

```javascript
// simplified using defaults
{
    "_op": "elasticsearch_reader",
    "index": "events-*",
    "type": "event",
    "size": 5000,
    "date_field_name": "created"
}

//expanded
{
    "_op": "elasticsearch_reader",
    "index": "events-*",
    "type": "event",
    "size": 5000,
    "start": "2015-10-26T21:33:27.190-07:00",
    "end": ""2015-10-27T21:33:27.190-07:00",
    "interval": "10min",
    "time_resolution": "s",
    "date_field_name": "created",
    "query": "someLucene: query",
    "subslice_by_key": true,
    "subslice_key_threshold": 100000,
    "key_type": base64url,
    "geo_field": "location",
    "geo_point": "33.4484,112.0740",
    "geo_distance": "100km",
    "geo_sort_order": "asc",
    "geo_sort_unit": "km"
}
```


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
            "_op": "elasticsearch_reader",
            "index": "test_index",
            "field": "uuid",
            "size": 1000,
            "key_type": "base64url",
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
            "field": "uuid",
            "size": 1000,
            "key_type": "base64url",
            "connection": "default"
        }
    ],
    "operations" : [
        {
            "_op" : "id_reader",
            "api_name" : "elasticsearch_reader_api"
        },
        {
            "_op": "noop"
        }
    ]
}
```

`NOTE`: If start with the long form (which means to set up the apis manually and specify the api_name on the operation) then there are restrictions on what configurations you can put on the operation so that clashing of configurations are minimalized. The api configs take precendence.

If submitting the job in long form, here is a list of parameters that will throw an error if also specified on the opConfig, since these values should be placed on the api:
- `index`
