import { OperationAPI } from '@terascope/job-components';
import {
    isNil, isString, isPlainObject, getTypeOf, AnyObject
} from '@terascope/utils';
import elasticApi from '@terascope/elasticsearch-api';
import { ReaderConfig, ValidReaderConfig } from './interfaces';

export default class ElasticsearchReaderApi extends OperationAPI {
    validateConfig(config: unknown): ValidReaderConfig {
        if (isNil(config)) throw new Error('No configuration was found or provided for elasticsearch_reader_api');
        if (!isObject(config)) throw new Error(`Invalid config, must be an object, was given ${getTypeOf(config)}`);
        if (isNil(config.connection) || !isString(config.connection)) throw new Error('Invalid parameter "connection", must provide a valid connection');
        return config as ValidReaderConfig;
    }

    async createAPI(config: ReaderConfig): Promise<elasticApi.Client> {
        const clientConfig = this.validateConfig(Object.assign({}, this.apiConfig, config));

        const { client } = this.context.foundation.getConnection({
            endpoint: clientConfig.connection,
            type: 'elasticsearch',
            cached: true
        });

        return elasticApi(client, this.context.logger, clientConfig);
    }
}

function isObject(input: unknown): input is AnyObject {
    return isPlainObject(input);
}
