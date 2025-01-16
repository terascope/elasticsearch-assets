import {
    ConvictSchema, AnyObject, cloneDeep,
    ValidatedJobConfig, isObjectEntity, getTypeOf
} from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import { SpacesAPIConfig } from '@terascope/elasticsearch-asset-apis';
import { schema } from '../elasticsearch_reader_api/schema.js';
import { DEFAULT_API_NAME } from './interfaces.js';

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
        default: '2 minutes',
        format: 'duration'
    },
    headers: {
        doc: 'Object containing headers for the requests',
        default: {},
        format: (val: unknown) => {
            if (!isObjectEntity(val)) {
                throw new Error(`Invalid parameter headers, must provide an object, was given ${getTypeOf(val)}`);
            }
        }
    },
    retry: {
        doc: 'The number of times that the spaces client will try to retry a request',
        default: 3,
        format: Number
    },
    caCertificate: {
        doc: 'CA certificate used to validate https endpoint',
        default: undefined,
        format: String
    },
    includeTotals: {
        doc: 'By default, data fetching is optimized by disabling total count calculation to achieve '
            + 'faster query execution. If you require total counts in your queries set this value to true '
            + `Some endpoints support setting to a fixed integer to limit the count up to that number then `
            + `stop... set to 'number' to count up to the query or slice size then stop. `,
        default: false,
        format(val: unknown) {
            if (val === 'number' || typeof val !== 'number') return;
            throw new Error(`Invalid parameter includeTotals, must be a boolean or string 'number', got ${getTypeOf(val)}`);
        }
    },
    optimizeCount: {
        doc: 'Whether to direct the request to an optimized API',
        default: undefined,
        format: Boolean
    },
};

const spacesSchema = Object.assign({}, clone, apiSchema) as AnyObject;

// this should not continue onward
delete spacesSchema.api_name;

spacesSchema.date_field_name.format = 'required_String';

export default class Schema extends ConvictSchema<SpacesAPIConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const { logger } = this.context;

        const apiConfigs = job.apis.filter((config) => {
            const apiName = config._name;
            return apiName === DEFAULT_API_NAME || apiName.startsWith(`${DEFAULT_API_NAME}:`);
        });

        apiConfigs.forEach((apiConfig: AnyObject) => {
            elasticAPI({}, logger).validateGeoParameters(apiConfig);
        });
    }

    build(): AnyObject {
        return spacesSchema;
    }
}
