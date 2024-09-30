import {
    ConvictSchema, ValidatedJobConfig, get,
    AnyObject, isString, getTypeOf, cloneDeep,
    isNumber, isNotNil
} from '@terascope/job-components';
import { ElasticsearchBulkConfig } from './interfaces.js';
import { DEFAULT_API_NAME } from '../elasticsearch_sender_api/interfaces.js';

export const schema: AnyObject = {
    size: {
        doc: 'the maximum number of docs it will take at a time, anything past it will be split up and sent'
            + 'note that the value should be even, the first doc will be the index data and then the next is the data',
        default: 500,
        format(val: unknown): void {
            if (!isNumber(val)) throw new Error(`Invalid parameter size, it must be of type number, was given ${getTypeOf(val)}`);
            if (isNaN(val)) throw new Error('Invalid size parameter, must be a number');
            if (val <= 0) throw new Error('Invalid size parameter, must be greater than zero');
        }
    },
    connection: {
        doc: 'Name of the elasticsearch connection to use when sending data.',
        default: 'default',
        format: 'optional_String'
    },
    index: {
        doc: 'Which index to read from',
        default: null,
        format(val: unknown): void {
            if (isNotNil(val)) {
                if (!isString(val)) throw new Error('Invalid index parameter, must be of type string');
                if (val.length === 0) throw new Error('Invalid index parameter, must not be an empty string');
                if (val.match(/[A-Z]/)) throw new Error('Invalid index parameter, must be lowercase');
            }
        }
    },
    type: {
        doc: 'Set the elasticsearch mapping type, required for elasticsearch v5 or lower, accepted in v6, and depreciated in v7 or above',
        default: '_doc',
        format: 'optional_String'
    },
    delete: {
        doc: 'Use the id_field from the incoming records to bulk delete documents.',
        default: false,
        format: Boolean
    },
    update: {
        doc: 'Specify if the data should update the records or, if not it will index them',
        default: false,
        format: Boolean
    },
    update_retry_on_conflict: {
        doc: 'If there is a version conflict from an update how often should it be retried.',
        default: 0,
        format(val: unknown): void {
            if (!isNumber(val)) throw new Error(`Invalid parameter update_retry_on_conflict, it must be of type number, was given ${getTypeOf(val)}`);
            if (isNaN(val)) throw new Error('Invalid update_retry_on_conflict configuration, must be a number');
            if (val < 0) throw new Error('Invalid update_retry_on_conflict configuration, must be greater than or equal to zero');
        }
    },
    update_fields: {
        doc: 'if you are updating the documents, you can specify fields to update here (it should be an array '
            + 'containing all the field names you want updated), it defaults to sending the entire document',
        default: [],
        format: Array
    },
    upsert: {
        doc: 'Specify if the incoming records should be used to perform an upsert. If update_fields is also specified then existing records will be updated with those fields otherwise the full incoming record will be inserted.',
        default: false,
        format: Boolean
    },
    create: {
        doc: 'Specify if the incoming records should be used to perform an create event ("put-if-absent" behavior).',
        default: false,
        format: Boolean
    },
    script_file: {
        doc: 'Name of the script file to run as part of an update request.',
        default: '',
        format: 'optional_String'
    },
    script: {
        doc: 'Inline script to include in each indexing request. Only very simple painless scripts are currently supported.',
        default: '',
        format: 'optional_String'
    },
    script_params: {
        doc: 'key -> value parameter mappings. The value will be extracted from the incoming data and passed to the script as param based on the key.',
        default: {},
        format: Object
    }
};

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
        const clone = cloneDeep(schema);
        clone.api_name = {
            doc: 'name of api to be used by elasticsearch reader',
            default: DEFAULT_API_NAME,
            format: (val: unknown): void => {
                if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
                if (!val.includes(DEFAULT_API_NAME)) throw new Error('Invalid parameter api_name, it must be an elasticsearch_sender_api');
            }
        };
        return clone;
    }
}
