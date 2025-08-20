import {
    ConvictSchema, ValidatedJobConfig, get,
    AnyObject, isString, getTypeOf,
} from '@terascope/job-components';
import { ElasticsearchBulkConfig } from './interfaces.js';
import { DEFAULT_API_NAME } from '../elasticsearch_sender_api/interfaces.js';

export default class Schema extends ConvictSchema<ElasticsearchBulkConfig> {
    validateJob(job: ValidatedJobConfig): void {
        let opIndex = 0;

        const opConfig = job.operations.find((op, ind) => {
            if (op._op === 'elasticsearch_bulk') {
                opIndex = ind;
                return op;
            }
            return false;
        });

        if (opConfig == null) {
            throw new Error('Could not find elasticsearch_bulk operation in jobConfig');
        }

        const elasticConnectors = get(this.context, 'sysconfig.terafoundation.connectors.elasticsearch-next');
        if (elasticConnectors == null) {
            throw new Error('Could not find elasticsearch connector in terafoundation config');
        }

        const {
            api_name, ...newConfig
        } = opConfig;

        const apiName = api_name || `${DEFAULT_API_NAME}:${opConfig._op}-${opIndex}`;

        this.ensureAPIFromConfig(apiName, job, newConfig);
    }

    build(): AnyObject {
        return {
            api_name: {
                doc: 'name of api to be used by elasticsearch reader',
                default: DEFAULT_API_NAME,
                format: (val: unknown): void => {
                    if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
                    if (!val.includes(DEFAULT_API_NAME)) throw new Error('Invalid parameter api_name, it must be an elasticsearch_sender_api');
                }
            }
        };
    }
}
