import { APIFactory } from '@terascope/job-components';
import {
    isNil, isString, isPlainObject, getTypeOf, AnyObject, isNumber
} from '@terascope/utils';
import { createSpacesApi, SpacesConfig, BaseApi } from '@terascope/elasticsearch-asset-apis';

export default class SpacesReaderApi extends APIFactory<BaseApi, SpacesConfig > {
    // TODO: this needs more validation
    validateConfig(config: unknown): SpacesConfig {
        if (isNil(config)) throw new Error('No configuration was found or provided for elasticsearch_reader_api');
        if (!isObject(config)) throw new Error(`Invalid config, must be an object, was given ${getTypeOf(config)}`);
        if (isNil(config.connection) || !isString(config.connection)) throw new Error('Invalid parameter "connection", must provide a valid connection');
        if (isNil(config.index) || !isString(config.index)) throw new Error('Invalid parameter "index", must provide a valid index');
        if (isNil(config.token) || !isString(config.token)) throw new Error(`Invalid parameter "token", it must be of type string, received ${getTypeOf(config.token)}`);
        if (isNil(config.timeout) || !isNumber(config.timeout)) throw new Error(`Invalid parameter "timeout", it must be of type number, received ${getTypeOf(config.timeout)}`);

        return config as SpacesConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<SpacesConfig>
    ): Promise<{ client: BaseApi, config: SpacesConfig }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));
        const emitter = this.context.apis.foundation.getSystemEvents();
        const spacesArgs = { config, logger: this.logger, emitter };

        const client = await createSpacesApi(spacesArgs);

        return { client, config };
    }

    async remove(_index: string): Promise<void> {}
}

function isObject(input: unknown): input is AnyObject {
    return isPlainObject(input);
}
