
import {
    ConvictSchema, isString, ValidatedJobConfig, getOpConfig
} from '@terascope/job-components';
import { ESIDReaderConfig, IDType } from './interfaces';

export default class Schema extends ConvictSchema<ESIDReaderConfig> {
    validateJob(job: ValidatedJobConfig) {
        const opConfig = getOpConfig(job, 'id_reader');
        if (opConfig == null) throw new Error('could not find elasticsearch_reader operation in jobConfig');

        if (opConfig.key_range && job.slicers > opConfig.key_range.length) {
            throw new Error('The number of slicers specified on the job cannot be more the length of key_range');
        }

        if (opConfig.key_type === 'base64url') {
            if (job.slicers > 64) {
                throw new Error('The number of slicers specified on the job cannot be more than 64');
            }
        }

        if (opConfig.key_type === 'hexadecimal' || opConfig.key_type === 'HEXADECIMAL') {
            if (job.slicers > 16) {
                throw new Error('The number of slicers specified on the job cannot be more than 16');
            }
        }
    }

    build() {
        return {
            index: {
                doc: 'Which index to read from',
                default: '',
                format: 'required_String'

            },
            size: {
                doc: 'The keys will attempt to recurse until the chunk will be <= size',
                default: 10000,
                format(val: any) {
                    if (isNaN(val)) {
                        throw new Error('size parameter for id_reader must be a number');
                    } else if (val <= 0) {
                        throw new Error('size parameter for id_reader must be greater than zero');
                    }
                }
            },
            field: {
                doc: 'The field in which searches will be queryed from',
                default: '',
                format: 'required_String'
            },
            full_response: {
                doc: 'Set to true to receive the full Elasticsearch query response including index metadata.',
                default: false,
                format: Boolean
            },
            key_type: {
                doc: 'The type of id used in index',
                default: 'base64url',
                format: Object.keys(IDType)
            },
            key_range: {
                doc: 'if provided, slicer will only recurse on these given keys',
                default: null,
                format(val: any) {
                    if (val) {
                        if (!Array.isArray(val) && val.length === 0) {
                            throw new Error('key_range for id_reader must be an array with length > 0');
                        }
                    }
                }
            },
            starting_key_depth: {
                doc: 'if provided, slicer will only produce keys with minimum length determined by this setting',
                default: 0,
                format(val: any) {
                    if (val) {
                        if (isNaN(val)) {
                            throw new Error('starting_key_depth parameter for id_reader must be a number');
                        } else if (val <= 0) {
                            throw new Error('starting_key_depth parameter for id_reader must be greater than zero');
                        }
                    }
                }
            },
            query: {
                doc: 'You may place a lucene query here, and the slicer will use it when making slices',
                default: '',
                format: 'optional_String'
            },
            fields: {
                doc: 'used to only return fields that you are interested in',
                default: null,
                format(val: any) {
                    if (val === null) return;
                    if (!Array.isArray(val)) {
                        throw new Error('Fields parameter must be an array');
                    }
                    if (!val.every(isString)) {
                        throw new Error('the values listed in the fields array must be of type string');
                    }
                }
            },
            connection: {
                default: 'default'
            }
        };
    }
}
