# id_reader #
This will slice and read documents from elasticsearch based off of their specific _uid on elasticsearch <= v5, or against a field for elasticsearch >= 6 using wildcard queries

Currently the id_reader and makes keys for base64url (elasticsearch native id generator) and hexidecimal. However at this point the hexidecimal only works if the keys are lowercase, future update will fix this

`NOTE`: this processor works by taking the key and checking the size to see if its digestable. If the count is too large it will extend the key to attempt to further divide up the data to digestable chunks. If the initial key was to small and the corresponding data count to big, it could potentially hurt your cluster and/or timeout the job since its trying to fetch the size of a really large number of records. If its in the tens of billions, usually setting it to `5` works. The higher the key_depth, the longer it take to finish to slice through all the permutations of keys possible, but it will be safer. Please know your data requirements when using this operator.

| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| \_op | Name of operation, it must reflect the exact name of the file | String | required |
| index | Which index to read from | String | required
| type | The type of the document that you are reading, used when a chuck is so large that it must be divided up by the documents \_id | String | only required for elasticsearch v5
| size | The limit to the number of docs pulled in a chunk, if the number of docs retrieved by the slicer exceeds this number, it will cause the slicer to recurse to provide a smaller batch | Number | optional, defaults to 5000
| key_type | Used to specify the key type of the \_ids of the documents being queryed | String | optional, defaults to elasticsearch id generator (base64url) |
| key_range | if provided, slicer will only recurse on these given keys | Array | optional |
| starting_key_depth | if provided, slicer will only produce keys with minimum length determined by this setting | Number | optional |
| fields | Used to restrict what is returned from elasticsearch. If used, only these fields on the documents are returned | Array | optional |
| query | specify any valid lucene query for elasticsearch to use in filtering| String | optional |
| api_name | name of api to be used by id reader | String | optional, defaults to 'elasticsearch_reader_api' |
| connection | Name of the elasticsearch connection to use when sending data | String | optional, defaults to the 'default' connection created for elasticsearch|
| field | The field on which we are searching against | String | required if running against elasticsearch >= v6, otherwise it is not needed on v5


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
            "_op": "id_reader",
            "index": "test_index",
            "field": "uuid",
            "size": 1000,
            "key_type": "base64url",
            "connection": "default"
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
            "_op" : "noop"
        }
    ]
}
```

`NOTE`: If start with the long form (which means to set up the apis manually and specify the api_name on the operation) then there are restrictions on what configurations you can put on the operation so that clashing of configurations are minimalized. The api configs take precendence.

If submitting the job in long form, here is a list of parameters that will throw an error if also specified on the opConfig, since these values should be placed on the api:
- `index`
