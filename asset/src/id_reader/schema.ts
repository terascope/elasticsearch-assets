import { ConvictSchema, ValidatedJobConfig } from '@terascope/job-components';
import { opSchema } from '../__lib/schema.js';
import { ESIDReaderConfig } from './interfaces.js';
import { isNil } from '@terascope/core-utils';

export default class Schema extends ConvictSchema<ESIDReaderConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = job.operations.find((op) => {
            if (op._op === 'id_reader') {
                return op;
            }
            return false;
        });

        if (opConfig == null) throw new Error('Could not find id_reader operation in jobConfig');

        const {
            _api_name, ...newConfig
        } = opConfig;

        if (isNil(newConfig.id_field_name)) {
            throw new Error('For operation id_reader, parameter "id_field_name" must be set');
        }
    }

    build(): Record<string, any> {
        return opSchema;
    }
}
