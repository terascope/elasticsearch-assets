import { APIFactory } from '@terascope/job-components';
import {
    isNil, isString, isPlainObject, getTypeOf, AnyObject, isNumber
} from '@terascope/utils';
import Reader from '../elasticsearch_reader_api/reader';
import SpacesClient from './client';
import { ApiConfig } from '../elasticsearch_reader/interfaces';

export default class SpacesReaderApi extends APIFactory<Reader, ApiConfig > {
    // TODO: this needs more validation
    validateConfig(config: unknown): ApiConfig {
        if (isNil(config)) throw new Error('No configuration was found or provided for elasticsearch_reader_api');
        if (!isObject(config)) throw new Error(`Invalid config, must be an object, was given ${getTypeOf(config)}`);
        if (isNil(config.connection) || !isString(config.connection)) throw new Error('Invalid parameter "connection", must provide a valid connection');
        if (isNil(config.index) || !isString(config.index)) throw new Error('Invalid parameter "index", must provide a valid index');
        if (isNil(config.token) || !isString(config.token)) throw new Error(`Invalid parameter "token", it must be of type string, received ${getTypeOf(config.token)}`);
        if (isNil(config.timeout) || !isNumber(config.timeout)) throw new Error(`Invalid parameter "timeout", it must be of type number, received ${getTypeOf(config.timeout)}`);

        return config as ApiConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<ApiConfig>
    ): Promise<{ client: Reader, config: ApiConfig }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));
        const mockedClient = new SpacesClient(config, this.logger);
        const reader = new Reader(config, mockedClient, this.logger);

        return { client: reader, config };
    }

    async remove(_index: string): Promise<void> {}
}

function isObject(input: unknown): input is AnyObject {
    return isPlainObject(input);
}
