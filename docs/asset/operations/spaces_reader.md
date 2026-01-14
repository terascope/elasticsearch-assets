# spaces_reader

The spaces_reader allows you to fetch data from a endpoint on a spaces server. This reader is a wrapper around the [elasticsearch_reader](./elasticsearch_reader.md) so it has all the same behaviors, functionality and requirements of that reader.

For this reader to function correctly the data must contain a date field with enough time resolution to slice the data into reasonably sized chunks and the data must be spread out through time. You must also have a valid space `endpoint` url and valid user `token` to communicate with the server.

Since this data is ordered, it can be sliced in parallel, and be read in parallel to have a very high throughput. If your index does not have a date field or for some reason the dates are all the same then this reader may not be able to process the data and you should consider using the [id_reader](./id_reader.md).

this is a [recoverable](https://terascope.github.io/teraslice/docs/management-apis/endpoints-json#post-v1jobsjobid_recover) reader, meaning that this job can be stopped, and then pick back up where it left off.

Fetched records will already have metadata associated with it, like the `_key` field. Please reference the [metadata section](#metadata) for more information.

## Usage

### Batch read a filtered subset of an index

Here is an example of of a job that will check the `query_index` and get records between start date(inclusive) and the `end` date (exclusive). This will also filter based on the `query` parameter. This will use the `es-1` connection configuration described in your [terafoundation connector config](https://terascope.github.io/teraslice/docs/configuration/overview#terafoundation-connectors)

Example Job

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
        "_name": "spaces_reader_api",
        "endpoint": "YOUR_ENDPOINT_HERE",
        "token": "YOUR_TOKEN_HERE",
        "index": "query_index",
        "field": "created",
        "query": "bytes:>=100",
        "start": "2020-08-04T10:00:00.000Z",
        "end": "2020-08-04T20:00:00.000Z"
      }
  ],
    "operations" : [
        {
            "_op": "spaces_reader",
            "_api_name": "spaces_reader_api"
        },
        {
            "_op": "noop"
        }
    ]
}
```

Here is a representation of what data is being returned from the additional lucene query along with the date range restrictions

```javascript
const elasticsearchIndexData = [
    { created: "2020-08-04T16:38:18.372Z", id: 1, bytes: 213 },
    { created: "2020-08-04T17:18:20.582Z", id: 2, bytes: 13 },
    { created: "2020-08-04T20:38:19.132Z", id: 3, bytes: 2455 },
    { created: "2020-08-04T20:00:00.745Z", id: 4, bytes: 453 }
]

// will be fetched and ordered from earliest to latest

const expected fetchResults = [
    { created: "2020-08-04T16:38:18.372Z", id: 1, bytes: 213 },
]
```

`Notes`: be careful to take note of the `start` and `end` dates you specify, especially the formatting (UTC vs ISO etc) compared to the data in elasticsearch. Make sure to use the same formatting so you can filter the data you want. If one date encapsulates time zones but the other doesn't, you could be several hours off and get the wrong results.

## Parameters

| Configuration | Description                                                   | Type   | Notes                                     |
| ------------- | ------------------------------------------------------------- | ------ | ----------------------------------------- |
| \_op          | Name of operation, it must reflect the exact name of the file | String | required                                  |  |
| _api_name     | name of api to be used by spaces reader                       | String | optional, defaults to 'spaces_reader_api' |
                       


In elasticsearch_assets v5, teraslice apis must be set within the job configuration. Teraslice will no longer automatically setup the api for you. All fields related to the api that were previously allowed on the operation config must be specified in the api config. Configurations for the api should no longer be set on the operation as they will be ignored. The api's `_name` must match the operation's `_api_name`.
