

# elasticsearch_data_generator #
## Deprecated, please use data_generator from standard assets
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


## Example Job

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
            "_op": "elasticsearch_data_generator",
            "size": 10000
        },
        {
            "_op": "elasticsearch_bulk",
            "index": "other_index",
            "size": 1000,
            "index": true
        }
    ],
}

```
