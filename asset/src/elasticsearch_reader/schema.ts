import {
    ConvictSchema,
    ValidatedJobConfig,
    AnyObject,
    getTypeOf,
    isNil
} from '@terascope/job-components';
import { elasticOpSchema } from '../__lib/schema';
import { ESReaderConfig } from './interfaces';
import { DEFAULT_API_NAME } from '../elasticsearch_reader_api/interfaces';

export default class Schema extends ConvictSchema<ESReaderConfig> {
    validateJob(job: ValidatedJobConfig): void {
        let opIndex = 0;

        const opConfig = job.operations.find((op, ind) => {
            if (op._op === 'elasticsearch_reader') {
                opIndex = ind;
                return op;
            }
            return false;
        });

        if (opConfig == null) throw new Error('Could not find elasticsearch_reader operation in jobConfig');

        const {
            api_name, field, ...newConfig
        } = opConfig;

        const apiName = api_name || `${DEFAULT_API_NAME}:${opConfig._op}-${opIndex}`;

        // we set the new apiName back on the opConfig so it can reference the unique name
        opConfig.api_name = apiName;

        if (field) {
            this.context.logger.warn('For operation elasticsearch_reader, parameter "field" is deprecated and will be removed in later versions, please use "id_field_name" instead');
            newConfig.id_field_name = field;
        }

        this.ensureAPIFromConfig(apiName, job, newConfig);

        const elasticsearchReaderAPI = job.apis.find((jobApi) => jobApi._name === apiName);

        if (isNil(elasticsearchReaderAPI)) throw new Error(`Could not find job api ${apiName}`);

        // we keep these checks here as it pertains to date_reader behavior
        if (isNil(elasticsearchReaderAPI.date_field_name)) {
            throw new Error(`Invalid parameter date_field_name, must be of type string, was given ${getTypeOf(opConfig.date_field_name)}`);
        }

        if (job.lifecycle === 'persistent') {
            if (elasticsearchReaderAPI.interval === 'auto') {
                throw new Error('Invalid interval parameter, must be manually set while job is in persistent mode');
            }

            if (elasticsearchReaderAPI.delay === 'auto') {
                throw new Error('Invalid delay parameter, must be manually set while job is in persistent mode');
            }
        }
    }

    build(): AnyObject {
        return elasticOpSchema;
    }
}
