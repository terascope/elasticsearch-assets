import {
    ConvictSchema,
    AnyObject,
    ValidatedJobConfig,
    toNumber,
    isString,
    cloneDeep
} from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import { ElasticsearchReaderAPIConfig, DEFAULT_API_NAME } from './interfaces';
import { schema } from '../elasticsearch_reader/schema';
import { schema as idSchema } from '../id_reader/schema';

const fullSchema = Object.assign({}, idSchema, schema) as AnyObject;
const clonedSchema = cloneDeep(fullSchema);

clonedSchema.index.format = 'required_String';

export default class Schema extends ConvictSchema<ElasticsearchReaderAPIConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const { logger } = this.context;
        const apiConfigs = job.apis.filter((config) => config._name.startsWith(DEFAULT_API_NAME));

        apiConfigs.forEach((apiConfig: AnyObject) => {
            elasticAPI({}, logger).validateGeoParameters(apiConfig);

            const { connection, field } = apiConfig;
            const subsliceByKey = apiConfig.subslice_by_key;

            const { connectors } = this.context.sysconfig.terafoundation;
            const endpointConfig = connectors.elasticsearch[connection];

            if (endpointConfig == null) throw new Error(`Could not find elasticsearch endpoint configuration for connection ${connection}`);

            const apiVersion = endpointConfig.apiVersion
                ? toNumber(endpointConfig.apiVersion.charAt(0))
                : 6;

            if (subsliceByKey) {
                const configType = apiConfig.type;
                if (apiVersion <= 5 && (configType == null || !isString(configType) || configType.length === 0)) throw new Error(`For elasticsearch apiVersion ${endpointConfig.apiVersion}, a type must be specified`);
                if (apiVersion > 5 && (field == null || !isString(field) || field.length === 0)) throw new Error('If subslice_by_key is set to true, the field parameter of the documents must also be set');
            }

            if (apiConfig.key_range && job.slicers > apiConfig.key_range.length) {
                throw new Error('The number of slicers specified on the job cannot be more the length of key_range');
            }

            if (apiConfig.key_type === 'base64url') {
                if (job.slicers > 64) {
                    throw new Error('The number of slicers specified on the job cannot be more than 64');
                }
            }

            if (apiConfig.key_type === 'hexadecimal' || apiConfig.key_type === 'HEXADECIMAL') {
                if (job.slicers > 16) {
                    throw new Error('The number of slicers specified on the job cannot be more than 16');
                }
            }
        });
    }

    build(): AnyObject {
        return clonedSchema;
    }
}
