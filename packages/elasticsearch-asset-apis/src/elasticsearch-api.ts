import { debugLogger, Logger } from '@terascope/utils';
import { EventEmitter } from 'events';
import { Client } from 'elasticsearch';
import { BaseReaderAPI } from './base-api';
import { SpacesAPIConfig } from './interfaces';

// TODO: fix config type
export interface ElasticsearchAPIArgs {
    config: SpacesAPIConfig,
    client: Client,
    logger?: Logger;
    emitter?: EventEmitter;
}

export async function createElasticsearchReaderAPI({
    config,
    client,
    logger = debugLogger('spaces-api'),
    emitter = new EventEmitter()
}: ElasticsearchAPIArgs): Promise<BaseReaderAPI> {
    return new BaseReaderAPI(config, client, emitter, logger);
}
