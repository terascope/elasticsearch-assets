# id_reader #
The id_readr reads data from an Elasticsearch index using an algorithm that partitions the data using regex queries. Although regex queries are a bit more taxing on the cluster, its a more reliable way of dividing up data.

The behaviour of this reader changes with the version of elasticsearch being searched.

For this reader to function correctly the [connection config](https://terascope.github.io/teraslice/docs/configuration/overview#terafoundation-connectors) must have an appropriatly set `apiVersion` for the elasticsearch connection. The behaviour changes according to the apiVersion

when searching against elasticsearch <= v5, it will query directly against the _uid (the elasticsearch _id of the record) so parameter `field` is NOT needed. However the `type` of the record must be set

when searching against elasticsearch >= 6,  parameter `field` IS needed. This field needs to have a string value of high uniqueness, like an id of some sort.

Currently the id_reader will makes keys for base64url (elasticsearch native id generator) and hexidecimal. However at this point the hexidecimal only works if the keys are lowercase, future updates will fix this.

this is a [recoverable](https://terascope.github.io/teraslice/docs/management-apis/endpoints-json#post-v1jobsjobid_recover) reader, meaning that this job can be stopped, and then pick back up where it left off.

## Usage

Example Connector Config, apiVersion defaults to 6.5
```yaml
terafoundation:
    environment: 'development'
    log_level: info
    connectors:
        elasticsearch:
            default:
                host:
                    - "localhost:9200"
            es7:
                host:
                    - "localhost:9201"
                apiVersion: "7.0",
            es5:
                host:
                    - "localhost:9202"
                apiVersion: "5.6"

teraslice:
    workers: 8
    master: true
    master_hostname: "127.0.0.1"
    port: 5678
    name: "localteracluster"
```

### Batch read the entire content of an index with elasticsearch v6 or newer and return filtered fields
This will run queries against the uuid field on the records to split them up. Since `fields` parameter was set, then only those fields of the records will be returned
```json
{
  "name": "ID_Reindex",
  "lifecycle": "once",
  "slicers": 1,
  "workers": 1,
  "assets": ["elasticsearch"],
  "operations": [
    {
      "_op": "id_reader",
      "index": "test_index",
      "field": "uuid",
      "fields": ["ip", "created", "bytes", "uuid"],
      "connection": "default"
    },
    {
      "_op": "noop",
    }
  ]
}
```

```javascript
const elasticsearchData = [
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
          "bytes" : 1856458
        }
      },
      {
        "_index" : "test_index",
        "_type" : "events",
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
        "_type" : "events",
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

// only "e" uuids are left
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
This will run queries against the uuid field on the records to split them up. The results will be filtered by the `query` parameter, and will only search against a subset of uuids specified in `key_range`.
```json
{
  "name": "ID_Reindex",
  "lifecycle": "once",
  "slicers": 1,
  "workers": 1,
  "assets": ["elasticsearch"],
  "operations": [
    {
      "_op": "id_reader",
      "index": "test_index",
      "field": "uuid",
      "key_range": ["a"],
      "query": "bytes:>= 1000",
      "connection": "default"
    },
    {
      "_op": "noop",
    }
  ]
}
```

```javascript
const elasticsearchData = [
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
      },
      {
        "_index" : "test_index",
        "_type" : "events",
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
        "_type" : "events",
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
  "name": "ID_Reindex",
  "lifecycle": "once",
  "slicers": 4
  "workers": 25,
  "assets": ["elasticsearch"],
  "operations": [
    {
      "_op": "id_reader",
      "index": "test_index",
      "field": "uuid",
      "connection": "default"
    },
    {
      "_op": "noop",
    }
  ]
}
```
`Note`: having too many slicers could potentially overwhelm your elasticsearch cluster since regex queries are a little more expensive to run

### Batch read the entire content of an index with elasticsearch v5 or older
This is a job that will run against an elasticsearch v5 cluster. This will query against the _uid (the elasticsearch _id of the record) that has `type` set to events

```json
{
  "name": "ID_Reindex",
  "lifecycle": "once",
  "slicers": 1,
  "workers": 1,
  "assets": ["elasticsearch"],
  "operations": [
    {
      "_op": "id_reader",
      "index": "test_index",
      "type": "events",
      "connection": "es5"
    },
    {
      "_op": "noop",
    }
  ]
}
```

```javascript
const elasticsearchData = [
    {
        _id : "anb11XABW6OedlSjjhbz"
        _type : "events",
        _source: {
            some: 'data'
        }
    },
    {
        _id : "bab11XABW6OedlSjjhbz"
        _type : "events",
        _source: {
            i: 'amB'
        }
    },
     {
        _id : "bdb11XABW6OedlSjjhbz"
        _type : "events",
        _source: {
            me: 'too'
        }
    }
]

// the keys are iterated sequentially so "a" _id records are first
const firstSliceResults = [
    { some: 'data' }
];

// "a" is done, now will process "b" _id records
const secondSliceResults = [
    {  i: 'amB' },
    { me: 'too' }
];
```

## Parameters

| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| \_op | Name of operation, it must reflect the exact name of the file | String | required |
| index | Which index to read from | String | required
| type | The type of the document that you are reading, used when a chuck is so large that it must be divided up by the documents \_id | String | only required for elasticsearch v5
| size | The limit to the number of docs pulled in a chunk, if the number of docs retrieved by the slicer exceeds this number, it will cause the slicer to recurse to provide a smaller batch | Number | optional, defaults to 5000
| key_type | Used to specify the key type of the \_ids of the documents being queryed | String | optional, defaults to elasticsearch id generator (base64url) may be set to `base64url`, `base64`, `hexadecimal`, `HEXADECIMAL` (which is uppercase version of hexadecimal)|
| key_range | if provided, slicer will only recurse on these given keys | Array | optional |
| starting_key_depth | if provided, slicer will only produce keys with minimum length determined by this setting | Number | optional |
| fields | Used to restrict what is returned from elasticsearch. If used, only these fields on the documents are returned | Array | optional |
| query | specify any valid lucene query for elasticsearch to use in filtering| String | optional |
| api_name | name of api to be used by id reader | String | optional, defaults to 'elasticsearch_reader_api' |
| connection | Name of the elasticsearch connection to use when sending data | String | optional, defaults to the 'default' connection created for elasticsearch|
| field | The field on which we are searching against | String | required if running against elasticsearch >= v6, otherwise it is not needed on v5

## Advanced Configuration

#### starting_key_depth
This processor works by taking a char from a list of possible chars for a given key_type (base64url) and checking the count of each char to see if its digestable.

If the count is too large it will extend the key_depth to attempt to further divide up the data to digestable chunks.
```
a =>
aa, ab, ac ...aK, ...a4, a_ =>
aaa, aab, aac ...aaK
```

It does this repeatedly until its comes to a digestable chunk.

If the initial key size was to small and its corresponding data count too big, it could potentially hurt your cluster and/or timeout the job since its trying to fetch the size of a really large number of records.

If its in the tens of billions, usually setting it to `5` works.

The higher the key_depth, the longer it take to finish to slice through all the permutations of keys possible, but it will be safer with larger data sets. Please know your data requirements when using this operator.

#### API usage in a job
In elasticsearch_assets v3, many core components were made into teraslice apis. When you use an elasticsearch processor it will automatically setup the api for you, but if you manually specify the api, then there are restrictions on what configurations you can put on the operation so that clashing of configurations are minimalized. The api configs take precendence.

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
