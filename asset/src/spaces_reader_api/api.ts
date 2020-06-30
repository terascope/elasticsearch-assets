import { APIFactory } from '@terascope/job-components';
import {
    isNil, isString, isPlainObject, getTypeOf, AnyObject
} from '@terascope/utils';
import elasticAPI from '@terascope/elasticsearch-api';
import SpacesClient from './client';
import { ValidReaderConfig } from '../elasticsearch_reader_api/interfaces';

export default class SpacesReaderApi extends APIFactory<elasticAPI.Client, AnyObject > {
    // TODO: this needs more validation
    validateConfig(config: unknown): ValidReaderConfig {
        if (isNil(config)) throw new Error('No configuration was found or provided for elasticsearch_reader_api');
        if (!isObject(config)) throw new Error(`Invalid config, must be an object, was given ${getTypeOf(config)}`);
        if (isNil(config.connection) || !isString(config.connection)) throw new Error('Invalid parameter "connection", must provide a valid connection');
        return config as ValidReaderConfig;
    }

    async create(
        _name: string, overrideConfigs: AnyObject
    ): Promise<{ client: elasticAPI.Client, config: AnyObject }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));
        // TODO: fixme: remove the any
        const mockedClient = new SpacesClient(config as any, this.logger);
        const client = elasticAPI(mockedClient, this.logger, config);

        return { client, config };
    }

    async remove(_index: string): Promise<void> {}
}

function isObject(input: unknown): input is AnyObject {
    return isPlainObject(input);
}
