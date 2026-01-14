# id_reader

The id_reader reads data from an elasticsearch index using an algorithm that partitions the data based on a string field of the record. This field value needs to have high uniqueness, like an id of some sort.

The behavior of this reader changes with the version of elasticsearch being searched.

For this reader to function correctly the parameter `id_field_name` is required. The [string field type](https://opensearch.org/docs/latest/field-types/supported-field-types/string/) of `id_field_name` should be `keyword`. A field type of `text` will be far less performant and will likely match on the same key multiple times, resulting in duplicate document reads. If the field type is `text` and was created by a dynamic mapping, it is possible to append `.keyword` to `id_field_name` to use the text field as a keyword. See this [blog post](https://www.elastic.co/blog/strings-are-dead-long-live-strings) for some background on why this works.

Currently the id_reader will makes keys for base64url (elasticsearch native id generator) and hexadecimal. However at this point the hexadecimal only works if the keys are lowercase, future updates will fix this.

this is a [recoverable](https://terascope.github.io/teraslice/docs/management-apis/endpoints-json#post-v1jobsjobid_recover) reader, meaning that this job can be stopped, and then pick back up where it left off.

## Usage


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
  "apis" : [
      {
          "_name": "elasticsearch_reader_api",
          "index": "test_index",
          "id_field_name": "uuid",
          "fields": ["ip", "created", "bytes", "uuid"],
          "_connection": "default"
      }
  ],
  "operations": [
    {
      "_op": "id_reader",
      "_api_name": "elasticsearch_reader_api"
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
  "apis" : [
      {
          "_name": "elasticsearch_reader_api",
          "index": "test_index",
          "id_field_name": "uuid",
          "key_range": ["a"],
          "query": "bytes:>= 1000",
          "_connection": "default"
      }
  ],
  "operations": [
    {
      "_op": "id_reader",
      "_api_name": "elasticsearch_reader_api"
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
  "apis" : [
      {
          "_name": "elasticsearch_reader_api",
          "index": "test_index",
          "id_field_name": "uuid",
          "_connection": "default"
      }
  ],
  "operations": [
    {
      "_op": "id_reader",
      "_api_name": "elasticsearch_reader_api"
    },
    {
      "_op": "noop"
    }
  ]
}
```

`Note`: having too many slicers could potentially overwhelm your elasticsearch cluster since regex queries are a little more expensive to run

## Parameters

| Configuration | Description                                                   | Type   | Notes    |
| ------------- | ------------------------------------------------------------- | ------ | -------- |
| \_op          | Name of operation, it must reflect the exact name of the file | String | required |
| _api_name     | name of api to be used by id reader                           | String | required |
|               |

In elasticsearch_assets v5, teraslice apis must be set within the job configuration. Teraslice will no longer automatically setup the api for you. All fields related to the api that were previously allowed on the operation config must be specified in the api config. Configurations for the api should no longer be set on the operation as they will be ignored. The api's `_name` must match the operation's `_api_name`.
