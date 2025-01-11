import {
    ConvictSchema, AnyObject, cloneDeep,
    ValidatedJobConfig, isObjectEntity, getTypeOf
} from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import { SpacesAPIConfig } from '@terascope/elasticsearch-asset-apis';
import { schema } from '../elasticsearch_reader_api/schema.js';
import { DEFAULT_API_NAME } from './interfaces.js';
import { ElasticsearchDistribution } from '@terascope/types';

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
    include_total: {
        doc: `Fetch total count in queries. Setting to 'false' will turn off calculation. `
            + `Some clients support setting to a fixed integer to limit the count up to that number `
            + `then stop... set to 'number' to count up to the slice size then stop. `
            + 'This can be used to achieve faster query execution.',
        default: true,
        format(val: unknown) {
            if (val === 'number' || typeof val !== 'number') return;
            throw new Error(`Invalid parameter include_total, must be a boolean or string 'number', got ${getTypeOf(val)}`);
        }
    },
    clientMetadata: {
        doc: 'Additional information if not using Elasticsearch 6',
        default: { version: 6, distribution: ElasticsearchDistribution.elasticsearch },
        format: Object
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
