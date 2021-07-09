import { debugLogger, Logger } from '@terascope/utils';
import { EventEmitter } from 'events';
import { ElasticsearchReaderAPI } from './elasticsearch-reader-api';
import { ESReaderOptions } from './interfaces';
import { ReaderClient } from './reader-client';

export interface ElasticsearchAPIArgs {
    config: ESReaderOptions,
    client: ReaderClient,
    logger?: Logger;
    emitter?: EventEmitter;
}

export function createElasticsearchReaderAPI({
    config,
    client,
    logger = debugLogger('elasticsearch-api'),
    emitter = new EventEmitter()
}: ElasticsearchAPIArgs): ElasticsearchReaderAPI {
    return new ElasticsearchReaderAPI(config, client, emitter, logger);
}
