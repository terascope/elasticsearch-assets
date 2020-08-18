import { APIFactory } from '@terascope/job-components';
import {
    isNil, isString, isPlainObject, getTypeOf, AnyObject
} from '@terascope/utils';
import elasticAPI from '@terascope/elasticsearch-api';
import { ElasticsearchReaderConfig } from './interfaces';

export default class ElasticsearchReaderAPI extends APIFactory<elasticAPI.Client, AnyObject > {
    // TODO: this needs more validation
    validateConfig(config: unknown): ElasticsearchReaderConfig {
        if (isNil(config)) throw new Error('No configuration was found or provided for elasticsearch_reader_api');
        if (!isObject(config)) throw new Error(`Invalid config, must be an object, was given ${getTypeOf(config)}`);
        if (isNil(config.connection) || !isString(config.connection)) throw new Error('Invalid parameter "connection", must provide a valid connection');
        return config as ElasticsearchReaderConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<ElasticsearchReaderConfig>
    ): Promise<{ client: elasticAPI.Client, config: AnyObject }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));

        const { client } = this.context.foundation.getConnection({
            endpoint: config.connection,
            type: 'elasticsearch',
            cached: true
        });
        const esClient = elasticAPI(client, this.logger, config);

        return { client: esClient, config };
    }

    async remove(_index: string): Promise<void> {}
}

function isObject(input: unknown): input is AnyObject {
    return isPlainObject(input);
}
