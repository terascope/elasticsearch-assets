import { debugLogger, Logger } from '@terascope/core-utils';
import { EventEmitter } from 'node:events';
import { ElasticsearchReaderAPI } from './ElasticsearchReaderAPI.js';
import {
    ESReaderOptions, FetchResponseType, ReaderClient, SpacesAPIConfig
} from './interfaces.js';
import { SpacesReaderClient } from './SpacesReaderClient.js';

export * from './ElasticsearchReaderAPI.js';
export * from './SpacesReaderClient.js';
export * from './ElasticsearchReaderClient.js';
export * from './interfaces.js';
export * from './WindowState.js';
export * from './algorithms/index.js';
export * from './utils.js';

export interface ElasticsearchAPIArgs {
    config: ESReaderOptions;
    client: ReaderClient;
    logger?: Logger;
    emitter?: EventEmitter;
}

/**
 * Creates an Elasticsearch Reader API with a Elasticsearch client
*/
export function createElasticsearchReaderAPI({
    config,
    client,
    logger = debugLogger('elasticsearch-api'),
    emitter = new EventEmitter()
}: ElasticsearchAPIArgs): ElasticsearchReaderAPI {
    return new ElasticsearchReaderAPI(config, client, emitter, logger);
}

interface SpacesAPIArgs {
    config: SpacesAPIConfig;
    logger?: Logger;
    emitter?: EventEmitter;
}

/**
 * Creates an Elasticsearch Reader API with a Spaces client
*/
export async function createSpacesReaderAPI({
    config,
    logger = debugLogger('spaces-api'),
    emitter = new EventEmitter()
}: SpacesAPIArgs): Promise<ElasticsearchReaderAPI> {
    const client = new SpacesReaderClient(config, logger);

    if (config.response_type === FetchResponseType.data_frame && !config.type_config) {
        config.type_config = await client.getDataType();
    }
    // simpleFetch should be used by the spaces reader to avoid the search size
    // expansion algorithm, this is because most endpoints are configured with
    // a much lower search limit than the backend clusters
    config.useSimpleFetch = true;

    return new ElasticsearchReaderAPI(config, client, emitter, logger);
}
