import {
    ConvictSchema,
    AnyObject,
    cloneDeep,
    ValidatedJobConfig,
    toNumber,
    isString
} from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import { ElasticsearchReaderAPIConfig, DEFAULT_API_NAME } from './interfaces';
import { schema } from '../elasticsearch_reader/schema';

const clonedSchema = cloneDeep(schema) as AnyObject;

clonedSchema.index.format = 'required_String';

export default class Schema extends ConvictSchema<ElasticsearchReaderAPIConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const { logger } = this.context;
        const apiConfigs = job.apis.filter((config) => config._name.startsWith(DEFAULT_API_NAME));

        apiConfigs.forEach((apiConfig: AnyObject) => {
            elasticAPI({}, logger).validateGeoParameters(apiConfig);

            const opConnection = apiConfig.connection;
            const subsliceByKey = apiConfig.subslice_by_key;
            const configField = apiConfig.field;

            const { connectors } = this.context.sysconfig.terafoundation;
            const endpointConfig = connectors.elasticsearch[opConnection];
            const apiVersion = endpointConfig.apiVersion
                ? toNumber(endpointConfig.apiVersion.charAt(0))
                : 6;

            if (subsliceByKey) {
                const configType = apiConfig.type;
                if (apiVersion <= 5 && (configType == null || !isString(configType) || configType.length === 0)) throw new Error(`For elasticsearch apiVersion ${endpointConfig.apiVersion}, a type must be specified`);
                if (apiVersion > 5 && (configField == null || !isString(configField) || configField.length === 0)) throw new Error('If subslice_by_key is set to true, the field parameter of the documents must also be set');
            }
        });
    }

    build(): AnyObject {
        return clonedSchema;
    }
}
