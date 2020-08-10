import {
    ConvictSchema,
    isString,
    ValidatedJobConfig,
    getOpConfig,
    AnyObject,
    getTypeOf,
    isNotNil,
    isNil,
    isNumber,
    toNumber
} from '@terascope/job-components';
import { ESIDReaderConfig, IDType } from './interfaces';
import { DEFAULT_API_NAME } from '../elasticsearch_reader_api/interfaces';
import { checkIndex } from '../elasticsearch_reader/schema';

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

        const { index, connection, api_name } = opConfig;
        if (!Array.isArray(job.apis)) job.apis = [];
        const ElasticReaderAPI = job.apis.find((jobApi) => jobApi._name === api_name);

        if (isNil(ElasticReaderAPI)) {
            checkIndex(opConfig.index);

            job.apis.push({
                _name: DEFAULT_API_NAME,
                index,
                connection,
                full_response: false
            });
        }

        const opConnection = ElasticReaderAPI ? ElasticReaderAPI.connection : opConfig.connection;
        const { connectors } = this.context.sysconfig.terafoundation;
        const endpointConfig = connectors.elasticsearch[opConnection];

        if (endpointConfig == null) throw new Error(`Could not find elasticsearch endpoint configuration for connection ${opConnection}`);
        if (endpointConfig.apiVersion) {
            const type = ElasticReaderAPI ? ElasticReaderAPI.type : opConfig.type;
            const versionNumber = toNumber(endpointConfig.apiVersion.charAt(0));
            if (versionNumber <= 5 && (type == null || !isString(type) || type.length === 0)) throw new Error(`For elasticsearch apiVersion ${endpointConfig.apiVersion}, a type must be specified`);
        }
    }

    build(): AnyObject {
        return {
            index: {
                doc: 'Which index to read from',
                default: null,
                format(val: unknown): void {
                    if (isNotNil(val)) checkIndex(val as any);
                }
            },
            size: {
                doc: 'The keys will attempt to recurse until the chunk will be <= size',
                default: 10000,
                format(val: unknown): void {
                    if (!isNumber(val)) throw new Error(`Invalid parameter size, it must be of type number, was given ${getTypeOf(val)}`);
                    if (isNaN(val)) throw new Error('Invalid size parameter, must be a number');
                    if (val <= 0) throw new Error('Invalid size parameter, must be greater than zero');
                }
            },
            field: {
                doc: 'The field in which searches will be queried from',
                default: null,
                format: 'optional_String'
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
                        if (Array.isArray(val)) {
                            if (val.length === 0) throw new Error('Invalid key_range parameter, must be an array with length > 0');
                            if (!val.every(isString)) throw new Error('Invalid key_range parameter, must be an array of strings');
                        } else {
                            throw new Error('Invalid key_range parameter, must be an array of strings');
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
                format(val: unknown): void {
                    if (isNotNil(val)) {
                        if (!Array.isArray(val)) throw new Error('Fields parameter must be an array');
                        if (!val.every(isString)) throw new Error('Invalid fields paramter, the values listed in the fields array must be of type string');
                    }
                }
            },
            connection: {
                doc: 'Name of the elasticsearch connection to use when sending data.',
                default: 'default',
                format: 'optional_String'
            },
            api_name: {
                doc: 'name of api to be used by elasticsearch reader',
                default: DEFAULT_API_NAME,
                format: (val: unknown) => {
                    if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
                    if (!val.includes(DEFAULT_API_NAME)) throw new Error('Invalid parameter api_name, it must be an elasticsearch_reader_api');
                }
            },
            type: {
                doc: 'Set the elasticsearch mapping type, required for elasticsearch v5 or lower, accepted in v6, and depreciated in v7 or above',
                default: null,
                format: 'optional_String'
            },
        };
    }
}
