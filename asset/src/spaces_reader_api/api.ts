import { APIFactory } from '@terascope/job-components';
import {
    isNil, isString, isPlainObject,
    getTypeOf, isNumber
} from '@terascope/core-utils';
import { createSpacesReaderAPI, SpacesAPIConfig, ElasticsearchReaderAPI } from '@terascope/elasticsearch-asset-apis';

export default class SpacesReaderAPI extends APIFactory<ElasticsearchReaderAPI, SpacesAPIConfig> {
    // TODO: this needs more validation
    validateConfig(config: unknown): SpacesAPIConfig {
        if (isNil(config)) {
            throw new Error('No configuration was found or provided for elasticsearch_reader_api');
        }
        if (!isObject(config)) {
            throw new Error(`Invalid config, must be an object, was given ${getTypeOf(config)}`);
        }
        if (isNil(config._connection) || !isString(config._connection)) {
            throw new Error('Invalid parameter "connection", must provide a valid connection');
        }
        if (isNil(config.index) || !isString(config.index)) {
            throw new Error('Invalid parameter "index", must provide a valid index');
        }
        if (isNil(config.token) || !isString(config.token)) {
            throw new Error(`Invalid parameter "token", it must be of type string, received ${getTypeOf(config.token)}`);
        }
        if (isNil(config.timeout) || !isNumber(config.timeout)) {
            throw new Error(`Invalid parameter "timeout", it must be of type number, received ${getTypeOf(config.timeout)}`);
        }

        return config as SpacesAPIConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<SpacesAPIConfig>
    ): Promise<{ client: ElasticsearchReaderAPI; config: SpacesAPIConfig }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));
        const emitter = this.context.apis.foundation.getSystemEvents();
        const spacesArgs = { config, logger: this.logger, emitter };

        const client = await createSpacesReaderAPI(spacesArgs);

        return { client, config };
    }

    async remove(_index: string): Promise<void> {}
}

function isObject(input: unknown): input is Record<string, any> {
    return isPlainObject(input);
}
