import { ConvictSchema, AnyObject } from '@terascope/job-components';
import { ESStateStorageConfig } from './interfaces';

export default class Schema extends ConvictSchema<ESStateStorageConfig> {
    build(): AnyObject {
        return {
            index: {
                doc: 'name of elasticsearch index',
                default: '',
                format: 'required_String'
            },
            type: {
                doc: 'type of the elasticsearch data string',
                default: '_doc',
                format: 'optional_String'
            },
            concurrency: {
                doc: 'number of concurrent requests to elasticsearch, defaults to 10',
                default: 10
            },
            source_fields: {
                doc: 'fields to retrieve from elasticsearch, array of fields, defaults to all fields',
                default: []
            },
            chunk_size: {
                doc: 'how many docs to send in the elasticsearch mget request at a time, defaults to 2500',
                default: 2500
            },
            persist: {
                doc: 'If set to true will save state in storage for mset, doest not apply to set, defaults to false',
                default: false
            },
            meta_key_field: {
                doc: 'Data entity meta field used for cache key and elasticsearch _id for mget/ mset functions',
                default: '_key'
            },
            connection: {
                doc: 'elasticsearch connection',
                default: 'default'
            },
            cache_size: {
                doc: 'max number of items to store in the cache (not memory size), defaults to 2147483647',
                default: (2 ** 31) - 1,
            },
        };
    }
}
