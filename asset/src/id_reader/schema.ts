import {
    ConvictSchema, isString, ValidatedJobConfig, getOpConfig, AnyObject
} from '@terascope/job-components';
import { ESIDReaderConfig, IDType } from './interfaces';

export default class Schema extends ConvictSchema<ESIDReaderConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = getOpConfig(job, 'id_reader');
        if (opConfig == null) throw new Error('Could not find elasticsearch_reader operation in jobConfig');

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

    build(): AnyObject {
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
                        throw new Error('Invalid size parameter, must be a number');
                    } else if (val <= 0) {
                        throw new Error('Invalid size parameter, must be greater than zero');
                    }
                }
            },
            field: {
                doc: 'The field in which searches will be queryed from',
                default: '',
                format: 'required_String'
            },
            full_response: {
                doc: 'used internally for api, must be set to true',
                default: true,
                format: (val: any) => {
                    if (val !== true) throw new Error('Parameter full_response must be set to true');
                }
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
                            throw new Error('Invalid key_range parameter, must be an array with length > 0');
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
                            throw new Error('Invalid starting_key_depth parameter, must be a number');
                        } else if (val <= 0) {
                            throw new Error('Invalid starting_key_depth parameter, must be greater than zero');
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
                        throw new Error('Invalid fields parameter, the values listed in the fields array must be of type string');
                    }
                }
            },
            connection: {
                default: 'default'
            }
        };
    }
}
