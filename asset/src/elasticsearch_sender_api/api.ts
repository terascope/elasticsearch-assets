import { APIFactory } from '@terascope/job-components';
import {
    isNil, isString, isPlainObject, AnyObject, isNumber, getTypeOf
} from '@terascope/utils';
import elasticAPI from '@terascope/elasticsearch-api';
import ElasticsearchSender from './bulk_send';
import { ElasticsearchSenderConfig } from './interfaces';

export default class ElasticsearchSenderAPI extends APIFactory
    <ElasticsearchSender, ElasticsearchSenderConfig> {
    // TODO: there might need more checks here
    validateConfig(config: unknown): ElasticsearchSenderConfig {
        if (isNil(config)) throw new Error('No configuration was found or provided for elasticsearch_reader_api');
        if (!isObject(config)) throw new Error(`Invalid config, must be an object, was given ${getTypeOf(config)}`);
        if (!isNumber(config.size)) throw new Error(`Invalid size parameter, expected number, got ${getTypeOf(config.size)}`);
        if (isNil(config.connection) || !isString(config.connection)) throw new Error('Invalid parameter "connection", must provide a valid connection');
        return config as ElasticsearchSenderConfig;
    }

    async create(
        _name: string, overrideConfig: Partial<ElasticsearchSenderConfig>
    ): Promise<{ client: ElasticsearchSender, config: ElasticsearchSenderConfig }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfig));

        const { client } = this.context.foundation.getConnection({
            endpoint: config.connection,
            type: 'elasticsearch',
            cached: true
        });

        const esClient = elasticAPI(client, this.context.logger, config);
        const esSender = new ElasticsearchSender(esClient, config);

        return { client: esSender, config };
    }

    async remove(_index: string): Promise<void> {}
}

function isObject(input: unknown): input is AnyObject {
    return isPlainObject(input);
}
