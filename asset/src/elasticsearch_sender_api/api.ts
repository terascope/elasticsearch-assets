import { APIFactory } from '@terascope/job-components';
import {
    isNil, isString, isPlainObject, AnyObject, isNumber, getTypeOf
} from '@terascope/utils';
import elasticApi from '@terascope/elasticsearch-api';
import ElasticsearchSender from './bulk_send';
import { SenderConfig, ValidSenderConfig } from './interfaces';

export default class ElasticsearchSenderApi extends APIFactory<ElasticsearchSender, AnyObject> {
    // TODO: there might need more checks here
    validateConfig(config: unknown): ValidSenderConfig {
        if (isNil(config)) throw new Error('No configuration was found or provided for elasticsearch_reader_api');
        if (!isObject(config)) throw new Error(`Invalid config, must be an object, was given ${getTypeOf(config)}`);
        if (!isNumber(config.size)) throw new Error(`Invalid size parameter, expected number, got ${getTypeOf(config.size)}`);
        if (isNil(config.connection) || !isString(config.connection)) throw new Error('Invalid parameter "connection", must provide a valid connection');
        return config as ValidSenderConfig;
    }

    async create(
        _name: string, overrideConfig: SenderConfig
    ): Promise<{ client: ElasticsearchSender, config: AnyObject }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfig));

        const { client } = this.context.foundation.getConnection({
            endpoint: config.connection,
            type: 'elasticsearch',
            cached: true
        });

        const esClient = elasticApi(client, this.context.logger, config);
        const esSender = new ElasticsearchSender(esClient, config);

        return { client: esSender, config };
    }

    async remove(_index: string): Promise<void> {}
}

function isObject(input: unknown): input is AnyObject {
    return isPlainObject(input);
}
