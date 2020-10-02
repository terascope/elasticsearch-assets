import {
    ConvictSchema,
    AnyObject,
    getTypeOf,
    cloneDeep,
    isNumber,
    ValidatedJobConfig
} from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import { schema } from '../elasticsearch_reader_api/schema';
import { ApiConfig } from '../elasticsearch_reader/interfaces';
import { DEFAULT_API_NAME } from './interfaces';

const clone = cloneDeep(schema);

const apiSchema = {
    endpoint: {
        doc: 'The base API endpoint to read from: i.e. http://yourdomain.com/api/v1',
        default: null,
        format: 'required_String'
    },
    token: {
        doc: 'API access token for making requests',
        default: null,
        format: 'required_String'
    },
    timeout: {
        doc: 'Time in milliseconds to wait for a connection to timeout.',
        default: 300000,
        format(val: unknown): void {
            if (!isNumber(val)) throw new Error(`Invalid parameter timeout, it must be of type number, was given ${getTypeOf(val)}`);
            if (isNaN(val)) throw new Error('Invalid timeout parameter, must be a number');
            if (val <= 0) throw new Error('Invalid timeout parameter, must be greater than zero');
        }
    }
};

const spacesSchema = Object.assign({}, clone, apiSchema) as AnyObject;

// this should not continue onward
delete spacesSchema.api_name;

spacesSchema.date_field_name.format = 'required_String';

export default class Schema extends ConvictSchema<ApiConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const { logger } = this.context;

        const apiConfigs = job.apis.filter((config) => config._name.startsWith(DEFAULT_API_NAME));

        apiConfigs.forEach((apiConfig: AnyObject) => {
            elasticAPI({}, logger).validateGeoParameters(apiConfig);
        });
    }

    build(): AnyObject {
        return spacesSchema;
    }
}
