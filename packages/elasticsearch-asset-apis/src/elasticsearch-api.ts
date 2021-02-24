import { debugLogger, Logger } from '@terascope/utils';
import { EventEmitter } from 'events';
import { Client } from 'elasticsearch';
import { BaseReaderAPI } from './base-api';
import { ESReaderOptions } from './interfaces';

export interface ElasticsearchAPIArgs {
    config: ESReaderOptions,
    client: Client,
    logger?: Logger;
    emitter?: EventEmitter;
}

export async function createElasticsearchReaderAPI({
    config,
    client,
    logger = debugLogger('elasticsearch-api'),
    emitter = new EventEmitter()
}: ElasticsearchAPIArgs): Promise<BaseReaderAPI> {
    return new BaseReaderAPI(config, client, emitter, logger);
}
