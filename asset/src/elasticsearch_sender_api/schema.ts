import {
    BaseSchema, ValidatedJobConfig, APIConfig, getOpConfig
} from '@terascope/job-components';
import { isNumber, getTypeOf } from '@terascope/core-utils';
import { ElasticsearchAPISenderConfig, DEFAULT_API_NAME } from './interfaces.js';
import { isValidIndex } from '../__lib/schema.js';

export const schema: Record<string, any> = {
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
    _connection: {
        doc: 'Name of the elasticsearch connection to use when sending data.',
        default: 'default',
        format: 'optional_string'
    },
    index: {
        doc: 'Which index to read from',
        default: null,
        format(val: unknown): void {
            isValidIndex(val);
        }
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
        format: 'optional_string'
    },
    script: {
        doc: 'Inline script to include in each indexing request. Only very simple painless scripts are currently supported.',
        default: '',
        format: 'optional_string'
    },
    script_params: {
        doc: 'key -> value parameter mappings. The value will be extracted from the incoming data and passed to the script as param based on the key.',
        default: {},
        format: Object
    }
};

export default class Schema extends BaseSchema<ElasticsearchAPISenderConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const apiConfigs = job.apis.filter((config) => {
            const apiName = config._name;
            return apiName === DEFAULT_API_NAME || apiName.startsWith(`${DEFAULT_API_NAME}:`);
        });

        const { connectors } = this.context.sysconfig.terafoundation;

        // hack to get around default connection check until schema updates and further discussion
        if (connectors['elasticsearch-next'].default == null && getOpConfig(job, 'routed_sender')) {
            this._applyRoutedSenderConnection(job, apiConfigs);
        }

        apiConfigs.forEach((apiConfig: Record<string, any>) => {
            const { _connection } = apiConfig;
            const endpointConfig = connectors['elasticsearch-next'][_connection];

            if (endpointConfig == null) {
                throw new Error(`Could not find elasticsearch-next endpoint configuration for _connection ${_connection}`);
            }
        });
    }

    // replaces default connection with routed sender connection
    // for ops that use the routed sender, should be removed once routed_sender schema or
    // implementation is updated
    _applyRoutedSenderConnection(job: ValidatedJobConfig, apiConfigs: APIConfig[]): void {
        job.operations.forEach((op) => {
            if (op._op === 'routed_sender') {
                apiConfigs.filter((config) => config._name === op._api_name && config._connection === 'default')
                    .forEach((config) => {
                        [config._connection] = Object.values(op.routing);
                    });
            }
        });
    }

    build(): Record<string, any> {
        return schema;
    }
}
