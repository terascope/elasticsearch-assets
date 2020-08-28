import { APIFactory } from '@terascope/job-components';
import {
    isNil,
    isString,
    isPlainObject,
    getTypeOf,
    AnyObject
} from '@terascope/utils';
import Reader from './reader';
import { ESReaderOptions } from '../elasticsearch_reader/interfaces';

export default class ElasticsearchReaderAPI extends APIFactory<Reader, AnyObject > {
    // TODO: this needs more validation
    validateConfig(config: unknown): ESReaderOptions {
        if (isNil(config)) throw new Error('No configuration was found or provided for elasticsearch_reader_api');
        if (!isObject(config)) throw new Error(`Invalid config, must be an object, was given ${getTypeOf(config)}`);
        if (isNil(config.connection) || !isString(config.connection)) throw new Error('Invalid parameter "connection", must provide a valid connection');
        if (isNil(config.index) || !isString(config.index)) throw new Error('Invalid parameter "index", must provide a valid index');
        return config as ESReaderOptions;
    }

    async create(
        _name: string, overrideConfigs: Partial<ESReaderOptions>
    ): Promise<{ client: Reader, config: AnyObject }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));
        const { connection } = config;
        const { client } = this.context.foundation.getConnection({
            endpoint: connection,
            type: 'elasticsearch',
            cached: true
        });

        const reader = new Reader(config, client, this.logger);

        return { client: reader, config };
    }

    async remove(_index: string): Promise<void> {}
}

function isObject(input: unknown): input is AnyObject {
    return isPlainObject(input);
}
