import { BaseSchema } from '@terascope/job-components';
import { isString, getTypeOf } from '@terascope/core-utils';
import { DEFAULT_API_NAME } from '../elasticsearch_sender_api/interfaces.js';
import { OpApiConfig } from '../__lib/interfaces.js';

export default class Schema extends BaseSchema<OpApiConfig> {
    build(): Record<string, any> {
        return {
            _api_name: {
                doc: 'name of api to be used by elasticsearch reader',
                default: DEFAULT_API_NAME,
                format: (val: unknown): void => {
                    if (!isString(val)) throw new Error(`Invalid parameter _api_name, it must be of type string, was given ${getTypeOf(val)}`);
                    if (!val.includes(DEFAULT_API_NAME)) throw new Error('Invalid parameter _api_name, it must be an elasticsearch_sender_api');
                }
            }
        };
    }
}
