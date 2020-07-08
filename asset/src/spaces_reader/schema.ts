import {
    ConvictSchema,
    AnyObject,
    isString,
    getTypeOf,
    getOpConfig,
    ValidatedJobConfig,
    isNil,
    isNumber,
    isNotNil
} from '@terascope/job-components';
import ReaderSchema from '../elasticsearch_reader/schema';
import { ApiConfig } from '../elasticsearch_reader/interfaces';
import { DEFAULT_API_NAME } from '../spaces_reader_api/interfaces';

export default class Schema extends ConvictSchema<ApiConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = getOpConfig(job, 'spaces_reader') as ApiConfig;
        const { api_name, ...apiConfig } = opConfig;
        if (!Array.isArray(job.apis)) job.apis = [];
        const SpacesReaderAPI = job.apis.find((jobApi) => jobApi._name === api_name);

        if (isNil(SpacesReaderAPI)) {
            if (isNil(opConfig.index) || !isString(opConfig.index)) throw new Error(`Invalid parameter index, must be of type string, was given ${getTypeOf(opConfig.index)}`);
            if (isNil(opConfig.date_field_name) || !isString(opConfig.date_field_name)) throw new Error(`Invalid parameter date_field_name, must be of type string, was given ${getTypeOf(opConfig.date_field_name)}`);
            if (isNil(opConfig.endpoint) || !isString(opConfig.endpoint)) throw new Error(`Invalid parameter endpoint, must be of type string, was given ${getTypeOf(opConfig.endpoint)}`);
            if (isNil(opConfig.token) || !isString(opConfig.token)) throw new Error(`Invalid parameter token, must be of type string, was given ${getTypeOf(opConfig.token)}`);
            if (isNil(opConfig.timeout) || !isNumber(opConfig.timeout)) throw new Error(`Invalid parameter timeout, must be of type number, was given ${getTypeOf(opConfig.timeout)}`);

            job.apis.push({
                _name: DEFAULT_API_NAME,
                ...apiConfig
            });
        } else {
            ['endpoint', 'index', 'token', 'index', 'date_field_name'].forEach((field) => {
                if (isNotNil(opConfig[field])) throw new Error(`Invalid config, if api is specified, parameter ${field} must live in the api config and not in spaces_reader`);
            });
        }
    }

    build(): AnyObject {
        const schema = new ReaderSchema(this.context, this.opType);
        const esSchema = schema.build();

        esSchema.api_name = {
            doc: 'name of api to be used by elasticearch reader',
            default: DEFAULT_API_NAME,
            format: (val: unknown) => {
                if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
                if (!val.includes(DEFAULT_API_NAME)) throw new Error('Invalid parameter api_name, it must be an spaces_reader_api');
            }
        };

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
        };

        return Object.assign({}, esSchema, apiSchema);
    }
}
