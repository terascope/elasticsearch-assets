import { BaseSchema, ValidatedJobConfig } from '@terascope/job-components';
import { opSchema } from '../__lib/schema.js';
import { ESIDReaderConfig } from './interfaces.js';
import { isNil } from '@terascope/core-utils';

export default class Schema extends BaseSchema<ESIDReaderConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const apiConfig = job.apis.find((api) => {
            if (api._name === 'elasticsearch_reader_api') {
                return api;
            }
            return false;
        });

        if (apiConfig == null) throw new Error('Could not find an elasticsearch_reader_api definition in the jobConfig');

        if (isNil(apiConfig.id_field_name)) {
            throw new Error('For the api connected to the id_reader, parameter "id_field_name" must be set');
        }
    }

    build(): Record<string, any> {
        return opSchema;
    }
}
