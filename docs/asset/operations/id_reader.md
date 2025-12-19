# id_reader

The id_reader reads data from an elasticsearch index using an algorithm that partitions the data based on a string field of the record. This field value needs to have high uniqueness, like an id of some sort.

The behavior of this reader changes with the version of elasticsearch being searched.

For this reader to function correctly the parameter `id_field_name` is required. The [string field type](https://opensearch.org/docs/latest/field-types/supported-field-types/string/) of `id_field_name` should be `keyword`. A field type of `text` will be far less performant and will likely match on the same key multiple times, resulting in duplicate document reads. If the field type is `text` and was created by a dynamic mapping, it is possible to append `.keyword` to `id_field_name` to use the text field as a keyword. See this [blog post](https://www.elastic.co/blog/strings-are-dead-long-live-strings) for some background on why this works.

Currently the id_reader will makes keys for base64url (elasticsearch native id generator) and hexadecimal. However at this point the hexadecimal only works if the keys are lowercase, future updates will fix this.

this is a [recoverable](https://terascope.github.io/teraslice/docs/management-apis/endpoints-json#post-v1jobsjobid_recover) reader, meaning that this job can be stopped, and then pick back up where it left off.

## Usage

Example Teraslice Config

```yaml
terafoundation:
    environment: 'development'
    log_level: info
    connectors:
        elasticsearch-next:
            default:
                node:
                    - "http://localhost:9200"

teraslice:
    workers: 8
    master: true
    master_hostname: "127.0.0.1"
    port: 5678
    name: "local_tera_cluster"
```

### Batch read the entire content of an index with elasticsearch v6 or newer and return filtered fields

This is an example of using the id_reader to run queries against the uuid field on the records to split them up. Since `fields` parameter was set, then only those fields of the records will be returned

Example Job:

```json
{
  "name": "ID_Reader",
  "lifecycle": "once",
  "slicers": 1,
  "workers": 1,
  "assets": ["elasticsearch"],
  "operations": [
    {
      "_op": "id_reader",
      "index": "test_index",
      "id_field_name": "uuid",
      "fields": ["ip", "created", "bytes", "uuid"],
      "connection": "default"
    },
    {
      "_op": "noop"
    }
  ]
}
```

Here is representation of elasticsearch data being sliced by the uuid field and returning only the fields requested

```javascript
// elasticsearch index data
[
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
          "bytes" : 1856458
        }
      },
      {
        "_index" : "test_index",
        "_type" : "_doc",
        "_id" : "mdyRQW4B8WLke7PkER8L",
        "_score" : 1.0,
        "_source" : {
          "ip" : "228.67.139.88",
          "url" : "http://marcus.info",
          "uuid" : "eedb2b6e-0256-458f-9d44-31cfe680824a",
          "created" : "2019-04-26T08:00:23.259-07:00",
          "ipv6" : "7ebe:e1c7:a43a:92f9:dbe6:b683:974a:d6db",
          "location" : "77.29129, -17.81098",
          "bytes" : 1828264
        }
      },
      {
        "_index" : "test_index",
        "_type" : "_doc",
        "_id" : "oNyRQW4B8WLke7PkER8L",
        "_score" : 1.0,
        "_source" : {
          "ip" : "43.234.54.76",
          "url" : "https://otto.biz",
          "uuid" : "ba2b53e0-b9e2-4717-93e9-a430e688bdb2",
          "created" : "2019-04-26T08:00:23.321-07:00",
          "ipv6" : "54b7:2eb2:8b34:ee1e:03cc:503c:38b6:063c",
          "location" : "73.71564, -170.41749",
          "bytes" : 3753310
        }
      }
]

// the keys are iterated sequentially so "a" _id records are first
const firstSliceResults = [
    {
        "ip" : "120.67.248.156",
        "created" : "2019-04-26T08:00:23.225-07:00",
        "bytes" : 1856458,
        "uuid" : "a23a8550-0081-453f-9e80-93a90782a5bd"
    }
];

// "a" is done, now will process "b" _id records
const secondSliceResults = [
    {
        "ip" : "43.234.54.76",
        "uuid" : "ba2b53e0-b9e2-4717-93e9-a430e688bdb2",
        "created" : "2019-04-26T08:00:23.321-07:00",
        "bytes" : 3753310
    }
];

// only "e" uuid's are left
const thirdSliceResults = [
    {
        "ip" : "228.67.139.88",
        "uuid" : "eedb2b6e-0256-458f-9d44-31cfe680824a",
        "created" : "2019-04-26T08:00:23.259-07:00",
        "bytes" : 1828264
    }
]
```

### Batch read the index with filter with key_range and query parameters

This will run queries against the uuid field on the records to split them up. The results will be filtered by the `query` parameter, and will only search against a subset of of the uuid specified in `key_range`.

Example Job:

```json
{
  "name": "ID_Reader",
  "lifecycle": "once",
  "slicers": 1,
  "workers": 1,
  "assets": ["elasticsearch"],
  "operations": [
    {
      "_op": "id_reader",
      "index": "test_index",
      "id_field_name": "uuid",
      "key_range": ["a"],
      "query": "bytes:>= 1000",
      "connection": "default"
    },
    {
      "_op": "noop"
    }
  ]
}
```

Here is representation of elasticsearch data being sliced and filtered by the query parameter

```javascript
const elasticsearchData = [
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
      },
      {
        "_index" : "test_index",
        "_type" : "_doc",
        "_id" : "mdyRQW4B8WLke7PkER8L",
        "_score" : 1.0,
        "_source" : {
          "ip" : "228.67.139.88",
          "url" : "http://marcus.info",
          "uuid" : "aedb2b6e-0256-458f-9d44-31cfe680824a",
          "created" : "2019-04-26T08:00:23.259-07:00",
          "ipv6" : "7ebe:e1c7:a43a:92f9:dbe6:b683:974a:d6db",
          "location" : "77.29129, -17.81098",
          "bytes" : 1856458
        }
      },
      {
        "_index" : "test_index",
        "_type" : "_doc",
        "_id" : "oNyRQW4B8WLke7PkER8L",
        "_score" : 1.0,
        "_source" : {
          "ip" : "43.234.54.76",
          "url" : "https://otto.biz",
          "uuid" : "ba2b53e0-b9e2-4717-93e9-a430e688bdb2",
          "created" : "2019-04-26T08:00:23.321-07:00",
          "ipv6" : "54b7:2eb2:8b34:ee1e:03cc:503c:38b6:063c",
          "location" : "73.71564, -170.41749",
          "bytes" : 3753310
        }
      }
]

// only a uuid that starts with "a" and has bytes >= 1000 are returned
const firstSliceResults = [
    {
        "ip" : "228.67.139.88",
        "url" : "http://marcus.info",
        "uuid" : "aedb2b6e-0256-458f-9d44-31cfe680824a",
        "created" : "2019-04-26T08:00:23.259-07:00",
        "ipv6" : "7ebe:e1c7:a43a:92f9:dbe6:b683:974a:d6db",
        "location" : "77.29129, -17.81098",
        "bytes" : 1856458
    }
];
```

### Higher Throughput Job

This will create 4 slicers that will divide up the the chars that it will search against, making more slices. The workers run each slice independently

```json
{
  "name": "ID_Reader",
  "lifecycle": "once",
  "slicers": 4,
  "workers": 25,
  "assets": ["elasticsearch"],
  "operations": [
    {
      "_op": "id_reader",
      "index": "test_index",
      "id_field_name": "uuid",
      "connection": "default"
    },
    {
      "_op": "noop"
    }
  ]
}
```

`Note`: having too many slicers could potentially overwhelm your elasticsearch cluster since regex queries are a little more expensive to run

## Parameters

| Configuration      | Description                                                                                                                                                                          | Type   | Notes                                                                                                           |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------ | --------------------------------------------------------------------------------------------------------------- |
| \_op               | Name of operation, it must reflect the exact name of the file                                                                                                                        | String | required                                                                                                        |
| index              | Which index to read from                                                                                                                                                             | String | required                                                                                                        |
| size               | The limit to the number of docs pulled in a chunk, if the number of docs retrieved by the slicer exceeds this number, it will cause the slicer to recurse to provide a smaller batch | Number | optional, defaults to 5000                                                                                      |
| key_type           | Used to specify the key type of the \_ids of the documents being queried                                                                                                             | String | optional, defaults to elasticsearch id generator (base64url) may be set to `base64url`, `base64`, `hexadecimal` |
| key_range          | if provided, slicer will only recurse on these given keys                                                                                                                            | Array  | optional                                                                                                        |
| starting_key_depth | if provided, slicer will only produce keys with minimum length determined by this setting                                                                                            | Number | optional                                                                                                        |
| fields             | Used to restrict what is returned from elasticsearch. If used, only these fields on the documents are returned                                                                       | Array  | optional                                                                                                        |
| query              | specify any valid lucene query for elasticsearch to use in filtering                                                                                                                 | String | optional                                                                                                        |
| _api_name          | name of api to be used by id reader                                                                                                                                                  | String | optional, defaults to 'elasticsearch_reader_api'                                                                |
| connection         | Name of the elasticsearch connection to use when sending data                                                                                                                        | String | optional, defaults to the 'default' connection created for elasticsearch                                        |
| id_field_name      | The field on which we are searching against                                                                                                                                          | String | required                                                                                                        |

## Advanced Configuration

### starting_key_depth

This processor works by taking a char from a list of possible chars for a given key_type (base64url) and checking the count of each char to see if its digestible.

If the count is too large it will extend the key_depth to attempt to further divide up the data to digestible chunks.

```sh
a =>
aa, ab, ac ...aK, ...a4, a_ =>
aaa, aab, aac ...aaK
```

It does this repeatedly until its comes to a digestible chunk.

If the initial key size was to small and its corresponding data count too big, it could potentially hurt your cluster and/or timeout the job since its trying to fetch the size of a really large number of records.

If its in the tens of billions, usually setting it to `5` works.

The higher the key_depth, the longer it take to finish to slice through all the permutations of keys possible, but it will be safer with larger data sets. Please know your data requirements when using this operator.

#### API usage in a job

In elasticsearch_assets v3, many core components were made into teraslice apis. When you use an elasticsearch processor it will automatically setup the api for you, but if you manually specify the api, then there are restrictions on what configurations you can put on the operation so that clashing of configurations are minimized. The api configs take precedence.

If submitting the job in long form, here is a list of parameters that will throw an error if also specified on the opConfig, since these values should be placed on the api:

- `index`

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
            "id_field_name": "uuid",
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
            "id_field_name": "uuid",
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
