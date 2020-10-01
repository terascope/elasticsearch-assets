import {
    ConvictSchema,
    AnyObject,
    isString,
    getTypeOf,
    ValidatedJobConfig,
    cloneDeep,
    mapValues,
    isNotNil
} from '@terascope/job-components';
import { schema } from '../elasticsearch_reader/schema';
import { ApiConfig } from '../elasticsearch_reader/interfaces';
import { DEFAULT_API_NAME } from '../spaces_reader_api/interfaces';
import { getNonDefaultValues } from '../__lib/helpers';

const clone = cloneDeep(schema);

const apiSchema = {
    endpoint: {
        doc: 'The base API endpoint to read from: i.e. http://yourdomain.com/api/v1',
        default: null,
        format: 'optional_String'
    },
    token: {
        doc: 'API access token for making requests',
        default: null,
        format: 'optional_String'
    },
    timeout: {
        doc: 'Time in milliseconds to wait for a connection to timeout.',
        default: 300000
    },
    api_name: {
        doc: 'name of api to be used by spaces reader',
        default: null,
        format: (val: unknown): void => {
            if (isNotNil(val)) {
                if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
                if (!val.includes(DEFAULT_API_NAME)) throw new Error('Invalid parameter api_name, it must be an spaces_reader_api');
            }
        }
    }
};

export const spacesSchema = Object.assign({}, clone, apiSchema);
const defaultSchema = mapValues<AnyObject>(spacesSchema, (obj) => obj.default);

export default class Schema extends ConvictSchema<ApiConfig> {
    validateJob(job: ValidatedJobConfig): void {
        let opIndex = 0;

        const opConfig = job.operations.find((op, ind) => {
            if (op._op === 'spaces_reader') {
                opIndex = ind;
                return op;
            }
            return false;
        });

        if (opConfig == null) throw new Error('Could not find spaces_reader operation in jobConfig');

        const { api_name, ...apiConfig } = opConfig;

        const uniqueSchemaValues = getNonDefaultValues(apiConfig, defaultSchema);
        const apiName = api_name || `${DEFAULT_API_NAME}:${opConfig._op}-${opIndex}`;

        // we set the new apiName back on the opConfig so it can reference the unique name
        opConfig.api_name = apiName;

        this.ensureAPIFromConfig(apiName, job, uniqueSchemaValues);
    }

    build(): AnyObject {
        return spacesSchema;
    }
}
