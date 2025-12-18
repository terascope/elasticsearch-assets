import { APIFactory } from '@terascope/job-components';
import {
    isNil, isString, isPlainObject,
    isNumber, getTypeOf
} from '@terascope/core-utils';
import elasticAPI from '@terascope/elasticsearch-api';
import {
    createElasticsearchBulkSender,
    ElasticsearchBulkSender
} from '@terascope/elasticsearch-asset-apis';
import { ElasticsearchAPISenderConfig } from './interfaces.js';

export default class ElasticsearchSenderAPI extends APIFactory
    <ElasticsearchBulkSender, ElasticsearchAPISenderConfig> {
    // TODO: there might need more checks here
    validateConfig(config: unknown): ElasticsearchAPISenderConfig {
        if (isNil(config)) {
            throw new Error('No configuration was found or provided for elasticsearch_reader_api');
        }
        if (!isObject(config)) {
            throw new Error(`Invalid config, must be an object, was given ${getTypeOf(config)}`);
        }
        if (!isNumber(config.size)) {
            throw new Error(`Invalid size parameter, expected number, got ${getTypeOf(config.size)}`);
        }
        if (isNil(config._connection) || !isString(config._connection)) {
            throw new Error('Invalid parameter "connection", must provide a valid connection');
        }
        return config as ElasticsearchAPISenderConfig;
    }

    async create(
        _name: string, overrideConfig: Partial<ElasticsearchAPISenderConfig>
    ): Promise<{ client: ElasticsearchBulkSender; config: ElasticsearchAPISenderConfig }> {
        const apiConfig = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfig));
        const { _api_name, ...config } = apiConfig;

        const { client } = await this.context.apis.foundation.createClient({
            endpoint: config._connection,
            type: 'elasticsearch-next',
            cached: true
        });

        const esClient = elasticAPI(client, this.context.logger, config);
        const sender = createElasticsearchBulkSender({ client: esClient, config });

        return { client: sender, config: apiConfig };
    }

    async remove(_index: string): Promise<void> {}
}

function isObject(input: unknown): input is Record<string, any> {
    return isPlainObject(input);
}
