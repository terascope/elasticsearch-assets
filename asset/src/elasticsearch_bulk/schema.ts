import { ConvictSchema, ValidatedJobConfig } from '@terascope/job-components';
import { isString, getTypeOf } from '@terascope/core-utils';
import { ElasticsearchBulkConfig } from './interfaces.js';
import { DEFAULT_API_NAME } from '../elasticsearch_sender_api/interfaces.js';

export default class Schema extends ConvictSchema<ElasticsearchBulkConfig> {
    // validateJob(job: ValidatedJobConfig): void {
    //     const opConfig = job.operations.find((op) => {
    //         if (op._op === 'elasticsearch_bulk') {
    //             return op;
    //         }
    //         return false;
    //     });

    //     if (opConfig == null) throw new Error('Could not find elasticsearch_reader operation in jobConfig');

    //     const { _api_name: apiName } = opConfig;

    //     const elasticsearchReaderAPI = job.apis.find((jobAPI) => jobAPI._name === apiName);

    //     if (isNil(elasticsearchReaderAPI)) throw new Error(`Could not find api: ${apiName} listed on the job`);

    //     // we keep these checks here as it pertains to date_reader behavior
    //     if (isNil(elasticsearchReaderAPI.date_field_name)) {
    //         throw new Error(`Invalid parameter date_field_name, must be of type string, was given ${getTypeOf(opConfig.date_field_name)}`);
    //     }

    //     if (job.lifecycle === 'persistent') {
    //         if (elasticsearchReaderAPI.interval === 'auto') {
    //             throw new Error('Invalid interval parameter, must be manually set while job is in persistent mode');
    //         }

    //         if (elasticsearchReaderAPI.delay === 'auto') {
    //             throw new Error('Invalid delay parameter, must be manually set while job is in persistent mode');
    //         }
    //     }
    // }

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
