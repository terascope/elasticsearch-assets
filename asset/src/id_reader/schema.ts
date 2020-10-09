import { ConvictSchema, ValidatedJobConfig, AnyObject } from '@terascope/job-components';
import { opSchema } from '../__lib/schema';
import { ESIDReaderConfig } from './interfaces';
import { DEFAULT_API_NAME } from '../elasticsearch_reader_api/interfaces';

export default class Schema extends ConvictSchema<ESIDReaderConfig> {
    validateJob(job: ValidatedJobConfig): void {
        let opIndex = 0;

        const opConfig = job.operations.find((op, ind) => {
            if (op._op === 'id_reader') {
                opIndex = ind;
                return op;
            }
            return false;
        });

        if (opConfig == null) throw new Error('Could not find id_reader operation in jobConfig');

        const {
            api_name, field, ...newConfig
        } = opConfig;

        const apiName = api_name || `${DEFAULT_API_NAME}:${opConfig._op}-${opIndex}`;

        // we set the new apiName back on the opConfig so it can reference the unique name
        opConfig.api_name = apiName;

        if (field) {
            this.context.logger.warn('For operation id_reader, parameter "field" is deprecated and will be removed in later versions, please use "id_field_name" instead');
            newConfig.id_field_name = field;
        }

        this.ensureAPIFromConfig(apiName, job, newConfig);
    }

    build(): AnyObject {
        return opSchema;
    }
}
