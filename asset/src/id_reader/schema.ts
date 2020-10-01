import {
    ConvictSchema,
    isString,
    ValidatedJobConfig,
    AnyObject,
    getTypeOf,
    isNotNil,
    isNumber,
    mapValues,
} from '@terascope/job-components';
import { ESIDReaderConfig, IDType } from './interfaces';
import { DEFAULT_API_NAME } from '../elasticsearch_reader_api/interfaces';
import { checkIndex } from '../elasticsearch_reader/schema';
import { getNonDefaultValues } from '../__lib/helpers';

export const schema = {
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
        format(val: unknown): void {
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
        format(val: unknown): void {
            if (val) {
                if (isNumber(val)) {
                    if (val <= 0) {
                        throw new Error('Invalid starting_key_depth parameter, must be greater than zero');
                    }
                } else {
                    throw new Error('Invalid starting_key_depth parameter, must be a number');
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
        doc: 'name of api to be used by id reader',
        default: null,
        format: (val: unknown):void => {
            if (isNotNil(val)) {
                if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
                if (!val.includes(DEFAULT_API_NAME)) throw new Error('Invalid parameter api_name, it must be an elasticsearch_reader_api');
            }
        }
    },
    type: {
        doc: 'Set the elasticsearch mapping type, required for elasticsearch v5 or lower, accepted in v6, and depreciated in v7 or above',
        default: null,
        format: 'optional_String'
    },
};

const defaultSchema = mapValues<AnyObject>(schema, (obj) => obj.default);

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
            api_name, ...newConfig
        } = opConfig;

        const uniqueSchemaValues = getNonDefaultValues(newConfig, defaultSchema);
        const apiName = api_name || `${DEFAULT_API_NAME}:${opConfig._op}-${opIndex}`;

        // we set the new apiName back on the opConfig so it can reference the unique name
        opConfig.api_name = apiName;

        this.ensureAPIFromConfig(apiName, job, uniqueSchemaValues);
    }

    build(): AnyObject {
        return schema;
    }
}
