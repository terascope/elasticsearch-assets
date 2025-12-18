import { ConvictSchema } from '@terascope/job-components';
import { isNumber, getTypeOf, isString } from '@terascope/core-utils';

import { ESStateStorageConfig } from './interfaces.js';

export default class Schema extends ConvictSchema<ESStateStorageConfig> {
    build(): Record<string, any> {
        return {
            index: {
                doc: 'name of elasticsearch index',
                default: '',
                format: 'required_string'
            },
            concurrency: {
                doc: 'number of concurrent requests to elasticsearch, defaults to 10',
                default: 10,
                format(val: unknown) {
                    if (!isNumber(val)) throw new Error(`Invalid parameter concurrency, must be a number, got ${getTypeOf(val)}`);
                    if (val <= 0) throw new Error('Invalid parameter concurrency, must be a number greater than zero');
                }
            },
            source_fields: {
                doc: 'fields to retrieve from elasticsearch, array of fields, defaults to all fields',
                default: [],
                format(val: unknown): void {
                    if (!Array.isArray(val)) throw new Error('Fields parameter must be an array');
                    if (!val.every(isString)) throw new Error('Invalid fields paramter, the values listed in the fields array must be of type string');
                }
            },
            chunk_size: {
                doc: 'how many docs to send in the elasticsearch mget request at a time, defaults to 2500',
                default: 2500,
                format(val: unknown) {
                    if (!isNumber(val)) throw new Error(`Invalid parameter chunk_size, must be a number, got ${getTypeOf(val)}`);
                    if (val <= 0) throw new Error('Invalid parameter chunk_size, must be a number greater than zero');
                }
            },
            persist: {
                doc: 'If set to true will save state in storage for mset, doest not apply to set, defaults to false',
                default: false,
                format: Boolean
            },
            meta_key_field: {
                doc: 'Data entity meta field used for cache key and elasticsearch _id for mget/ mset functions',
                default: '_key',
                format: 'required_string'
            },
            _connection: {
                doc: 'elasticsearch connection',
                default: 'default',
                format: 'required_string'
            },
            cache_size: {
                doc: 'max number of items to store in the cache (not memory size), defaults to 2147483647',
                default: (2 ** 31) - 1,
                format(val: unknown) {
                    if (!isNumber(val)) throw new Error(`Invalid parameter cache_size, must be a number, got ${getTypeOf(val)}`);
                    if (val <= 0) throw new Error('Invalid parameter cache_size, must be a number greater than zero');
                }
            },
        };
    }
}
