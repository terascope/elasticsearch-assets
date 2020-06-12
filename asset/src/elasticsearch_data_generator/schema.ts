import {
    ConvictSchema, ValidatedJobConfig, getOpConfig, AnyObject
} from '@terascope/job-components';
import { DataGenerator } from './interfaces';

export default class Schema extends ConvictSchema<DataGenerator> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = getOpConfig(job, 'elasticsearch_data_generator');
        if (!opConfig) throw new Error('No opConfig was found for operation elasticsearch_data_generator on the job');

        if (opConfig.id_start_key && !opConfig.set_id) {
            throw new Error('Invalid elasticsearch_data_generator configuration, id_start_key must be used with set_id parameter, please set the missing parameters');
        }

        if (opConfig.set_id) {
            const indexSelectorConfig = getOpConfig(job, 'elasticsearch_index_selector');
            if (!indexSelectorConfig) throw new Error('No opConfig was found for operation elasticsearch_index_selector on the job');

            if (!indexSelectorConfig.id_field) {
                throw new Error('Invalid elasticsearch_data_generator configuration, set_id must be used in tandem with id_field which is set in elasticsearch_index_selector');
            }

            if (indexSelectorConfig.id_field !== 'id') {
                throw new Error('Invalid elasticsearch_data_generator configuration, id_field must be set to "id" when elasticsearch_data_generator is creating ids');
            }
        }
    }

    build(): AnyObject {
        return {
            json_schema: {
                doc: 'file path to custom data schema',
                default: '',
                format: 'optional_String'
            },
            size: {
                doc: 'The limit to the number of docs pulled in a chunk, if the number of docs retrieved '
                + 'by the interval exceeds this number, it will cause the function to recurse to provide a smaller batch',
                default: 5000,
                format(val: any) {
                    if (isNaN(val)) {
                        throw new Error('Invalid size parameter for elasticsearch_data_generator, must be a number');
                    } else if (val <= 0) {
                        throw new Error('Invalid size parameter for elasticsearch_data_generator, must be greater than zero');
                    }
                }
            },
            start: {
                doc: 'The start date (ISOstring or in ms) to which it will read from ',
                default: '',
                format: 'optional_Date'
            },
            end: {
                doc: 'The end date (ISOstring or in ms) to which it will read to',
                default: '',
                format: 'optional_Date'
            },
            format: {
                doc: 'This is only used with the teraslice provided schema, can elect different time structures'
                + 'such as dateNow, utcDate, utcBetween and isoBetween',
                default: '',
                format: 'optional_String'
            },
            stress_test: {
                doc: 'used to speed up the creation process to test elasticsearch load',
                default: false,
                format: Boolean
            },
            date_key: {
                doc: 'key value on schema where date should reside',
                default: 'created',
                format: String
            },
            set_id: {
                doc: 'used to make an id on the data that will be used for the doc _id for elasticsearch, values: base64url, hexadecimal, HEXADECIMAL',
                default: '',
                format: 'optional_String'
            },
            id_start_key: {
                doc: 'set if you would like to force the first part of the ID to a certain character',
                default: '',
                format: 'optional_String'
            }
        };
    }
}
