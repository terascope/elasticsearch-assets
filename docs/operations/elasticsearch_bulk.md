# elasticsearch_bulk #
This operator sends bulk requests to elasticsearch


| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| \_op | Name of operation, it must reflect the exact name of the file | String | required |
| size | the maximum number of docs it will send in a given request, anything past it will be split up and sent | Number | required, typically the index selector returns up to double the length of the original documents due to the metadata involved with bulk requests. This number is essentially doubled to to maintain the notion that we split by actual documents and not the metadata |
| connection | Name of the elasticsearch connection to use when sending data | String | optional, defaults to the 'default' connection created for elasticsearch |
| index | Index to where the data will be sent to, it must be lowercase | String | required |
| type | Set the type of the data for elasticsearch | String | optional defaults to '_doc', is required for elasticsearch v5|
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
            "_op": "elasticsearch_bulk",
            "index": "other_index",
            "size": 1000,
            "index": true
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
            "_name": "elasticsearch_reader_api",
            "index": "test_index",
            "field": "uuid",
            "size": 1000,
            "key_type": "base64url",
            "connection": "default"
        },
        {
            "_name": "elasticsearch_sender_api",
            "index": "other_index",
            "size": 1000,
            "index": true
        }
    ],
    "operations" : [
        {
            "_op" : "id_reader",
            "api_name" : "elasticsearch_reader_api"
        },
         {
            "_op": "elasticsearch_bulk",
            "api_name" : "elasticsearch_sender_api"
        }
    ]
}
```

`NOTE`: If start with the long form (which means to set up the apis manually and specify the api_name on the operation) then there are restrictions on what configurations you can put on the operation so that clashing of configurations are minimalized. The api configs take precendence.

If submitting the job in long form, here is a list of parameters that will throw an error if also specified on the opConfig, since these values should be placed on the api:
- `index`
