## Readers ##

### elasticsearch_reader ###
Used to retrieve elasticsearch data based on dates. This reader has different behaviour if lifecycle is set to "once" or "persistent"

Example configuration if lifecycle is set to "once"

```
//simplified using defaults
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
    "time_resolution": "ms",
    "date_field_name": "created",
    "query": "someLucene: query",
    "full_response": true,
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
| full_response | If set to true, it will return the native response from elasticsearch with all meta-data included. If set to false it will return an array of the actual documents, no meta data included | Boolean | optional, defaults to false |
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

```
{
    "_op": "elasticsearch_reader",
    "index": "someindex",
    "size": 5000,
    "interval": "5s",
    "delay": "1m",
    "date_field_name": "created",
    "full_response": true
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

### spaces_reader ###
This is a wrapper around the elasticsearch_reader so it has all of the functionality, schemas and validations of that reader. This is used to allow client access to data through communication with a spaces server.

| Configuration | Description | Type |  Notes   |
| --------- | -------- | ------ | ------ |
| \_op | Name of operation, it must reflect the exact name of the file | String | required |
| endpoint | The base API endpoint to read from: i.e.http://yourdomain.com/api/v1 | String | required |
| token | teraserver API access token for making requests | String | required |
| timeout | Time in milliseconds to wait for a connection to timeout | Number | optional, defaults to 300000 ms or 5 mins  |
| api_name | name of api to be used by id reader | String | optional, defaults to 'spaces_reader_api' |

NOTE: this op uses all the configurations listed in the elasticsearch_reader in addtion to what is listed above!!! Please reference that reader. HOWEVER, a difference in behavior compared to the elasticsearch_reader is that the default geo distance sort will be ignored if any sort paramter is specified on the query. Sorting on geo distance while specifiying another sorting parameter is still possible if you set any other geo sorting parameter, which will cause the query to sort by both.


### elasticsearch_data_generator ###
Used to generate sample data for your elasticsearch cluster. You may use the default data generator which creates randomized data fitting the format listed below or you may create your own custom schema using the [mocker-data-generator](https://github.com/danibram/mocker-data-generator) package to create data to whatever schema you desire.

Default generated data :
```
{
    "ip": "1.12.146.136",
    "userAgent": "Mozilla/5.0 (Windows NT 5.2; WOW64; rv:8.9) Gecko/20100101 Firefox/8.9.9",
    "url": "https://gabrielle.org",
    "uuid": "408433ff-9495-4d1c-b066-7f9668b168f0",
    "ipv6": "8188:b9ad:d02d:d69e:5ca4:05e2:9aa5:23b0",
    "location": "-25.40587, 56.56418",
    "created": "2016-01-19T13:33:09.356-07:00",
    "bytes": 4850020
}

```

Example configuration
```
{
    "_op": "elasticsearch_data_generator",
    "size": 25000000,
    "json_schema": "some/path/to/file.js",
    "format": "isoBetween",
    "start": "2015-08-01",
    "end": "2015-12-30"
}
```
In once mode, this will created a total of 25 million docs with dates ranging from 2015-08-01 to 2015-12-30. The dates will appear in "2015-11-19T13:48:08.426-07:00" format.

| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| _op | Name of operation, it must reflect the exact name of the file | String | required |
| size | If lifecycle is set to "once", then size is the total number of documents that the generator will make. If lifecycle is set to "persistent", then this generator will will constantly stream  data to elasticsearch in chunks as big as the size indicated | Number | required |
| json_schema | File path to where custom schema is located | String | optional, the schema must be exported Node style "module.exports = schema" |
| format | specify any provided formats listed in /lib/utils/data_utils for the generator| String | optional, defaults to "dateNow" |
| start | start of date range | String | optional, only used with format isoBetween or utcBetween, defaults to Thu Jan 01 1970 00:00:00 GMT-0700 (MST) |
| end | end of date range | String | optional, only used with format isoBetween or utcBetween, defaults to new Date() |
| stress_test | If set to true, it will attempt to send non unique documents following your schema as fast as it can, originally used to help determine cluster write performance| Boolean | optional, defaults to false |
| date_key | Use this to indicate which key of your schema you would like to use a format listed below, just in case you don't want to set your own | String | optional, defaults to created |
| set_id | used to make an id on the data that will be used for the doc \_id for elasticsearch, values: base64url, hexadecimal, HEXADECIMAL | String | optional, if used, then index selector needs to have id_field set to "id" |
| id_start_key | set if you would like to force the first part of the ID to a certain character, adds a regex to the front | Sting | optional, must be used in tandem with set_id id_start_key is essentially regex, if you set it to "a", then the first character of the id will be "a", can also set ranges [a-f] or randomly alternate betweeen b and a if its set to "[ab]" |

#### Description of formats available ####
There are two categories of formats, ones that return the current date at which the function runs, or one that returns a date within a given range. Note for the non-range category, technically if the job takes 5 minutes to run, you will have dates ranging from the time you started the job up until the time it finished, so its still a range but not as one that spans hours, days weeks etc.


| Format | Description |
| --------- | -------- |
| dateNow | will create a new date in "2016-01-19T13:48:08.426-07:00" format, preserving local time |
| utcDate | will create a new utc date e.g "2016-01-19T20:48:08.426Z" |
| utcBetween | similar to utcDate, but uses start and end keys in the job config to specify range |
| isoBetween | similar to dateNow, but uses start and end keys in the job config to specify range |


#### persistent mode ####
 The data generator will continually stream data into elasticsearch, the "size" key" switches from the total number of documents created to how big each slice is when sent to elasticsearch

### id_reader ###
This will slice and read documents based off of their specific \_ids. Underneath the hood it does a wildcard query on \_uid

Example configuration
```
{
    "_op": "id_reader",
    "index": "events-2016.05.06",
    "type": "events",
    "size": 10000,
    "key_type": "hexadecimal",
    "key_range": ["a", "b", "c", "1"]
}

```
Currently the id_reader and makes keys for base64url (elasticsearch native id generator) and hexidecimal. However at this point the hexidecimal only works if the keys are lowercase, future update will fix this

| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| \_op | Name of operation, it must reflect the exact name of the file | String | required |
| index | Which index to read from | String | required
| type | The type of the document that you are reading, used when a chuck is so large that it must be divided up by the documents \_id | String | required
| size | The limit to the number of docs pulled in a chunk, if the number of docs retrieved by the slicer exceeds this number, it will cause the slicer to recurse to provide a smaller batch | Number | optional, defaults to 5000
| full_response | If set to true, it will return the native response from elasticsearch with all meta-data included. If set to false it will return an array of the actual documents, no meta data included | Boolean | optional, defaults to false |
| key_type | Used to specify the key type of the \_ids of the documents being queryed | String | optional, defaults to elasticsearch id generator (base64url) |
| key_range | if provided, slicer will only recurse on these given keys | Array | optional |
| starting_key_depth | if provided, slicer will only produce keys with minimum length determined by this setting | Number | optional |
| fields | Used to restrict what is returned from elasticsearch. If used, only these fields on the documents are returned | Array | optional |
| query | specify any valid lucene query for elasticsearch to use in filtering| String | optional |
| api_name | name of api to be used by id reader | String | optional, defaults to 'elasticsearch_reader_api' |

## Processors ##

### elasticsearch_bulk ###
This sends a bulk request to elasticsearch

Example configuration
```
{
    "_op": "elasticsearch_bulk",
    "date_field": "created"

    "size": 10000
}
```
The keys used were hexidecimal based

| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| \_op | Name of operation, it must reflect the exact name of the file | String | required |
| size | the maximum number of docs it will send in a given request, anything past it will be split up and sent | Number | required, typically the index selector returns up to double the length of the original documents due to the metadata involved with bulk requests. This number is essentially doubled to to maintain the notion that we split by actual documents and not the metadata |
| connection | Name of the elasticsearch connection to use when sending data | String | optional, defaults to the 'default' connection created for elasticsearch |
| index | Index to where the data will be sent to, it must be lowercase | String | required |
| type | Set the type of the data for elasticsearch | String | optional defaults to '_doc'|
| delete| Use the id_field from the incoming records to bulk delete documents | Boolean | optional, defaults to false |
| upsert| Specify if the incoming records should be used to perform an upsert. If update_fields is also specified then existing records will be updated with those fields otherwise the full incoming  record will be inserted | Boolean | optional, defaults to false |
| create| Specify if the incoming records should be used to perform an create event ("put-if-absent" behavior)| Boolean | optional, defaults to false |
| update | Specify if the data should update existing records, if false it will index them | Boolean | optional, defaults to false |
| update_fields | if you are updating the documents, you can specify fields to update here (it should be an array containing all the field names you want), it defaults to sending the entire document | Array | optional, defaults to [] |
| script_file | Name of the script file to run as part of an update request | String | optional |
| script | Inline script to include in each indexing request. Only very simple painless scripts are currently supported | String | optional |
| script_params | key -> value parameter mappings. The value will be extracted from the incoming data and passed to the script as param based on the key | Object | optional |
| update_retry_on_conflict | If there is a version conflict from an update how often should it be retried | Number | optional, defaults to 0 |
| api_name | name of api to be used by elasticearch bulk sender | String | optional, defaults to 'elasticsearch_sender_api' |

## APIS ##

The apis in this asset are the means to allow other custom made processors the ability to read and write to their respective destinations. When you use a reader or a sender from the asset, it will instantiate one for you automatically if you don't specify api_name.

Short hand method:

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
        "_op" : "elasticsearch_reader",
        "index" : "test_index",
        "size" : 5000,
        "date_field_name" : "created"
    },
    {
        "_op" : "elasticsearch_bulk",
        "size" : 10000,
        "index" : "api-test",
        "type" : "events"
    }
    ],
}

```

Will convert to =>

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
            "_name" : "elasticsearch_reader_api",
            "index" : "test_index",
            "connection" : "default",
            "full_response" : false
        },
        {
            "_name" : "elasticsearch_sender_api",
            "index" : "api-test",
            "connection" : "default",
            "size" : 10000
        }
    ],
    "operations" : [
        {
            "_op" : "elasticsearch_reader",
            "index" : "test_index",
            "size" : 5000,
            "date_field_name" : "created",
            "api_name": "elasticsearch_reader_api"
        },
        {
            "_op" : "elasticsearch_bulk",
            "size" : 10000,
            "index" : "api-test",
            "type" : "events",
            "api_name": "elasticsearch_sender_api"
        }
    ],
}
```

NOTE If start with the long form (which means to set up the apis manually and specify the api_name on the operation) then there are restrictions on what configurations you can put on the operation so that clashing of configurations are minimalized. The api configs take precendence.

If submitting the job in long form, here is a list of parameters that will throw an error, since these values will be placed on the api manually or by defaults

| operation | restricted fields |
| elasticsearch_reader | index |
| id_reader | index |
| elasticsearch_bulk | index |
| spaces_reader | index, endpoint, token, timeout, date_field_name |
