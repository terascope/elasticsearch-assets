import { debugLogger, Logger } from '@terascope/job-components';
import { EventEmitter } from 'events';
import { Client } from 'elasticsearch';
import BaseReaderAPI from './base-api';
import { SpacesApiConfig } from './interfaces';

interface ElasticsearchApiArgs {
    config: SpacesApiConfig,
    client: Client,
    logger?: Logger;
    emitter?: EventEmitter;
}

export default async function createElasticsearchApi({
    config,
    client,
    logger = debugLogger('spaces-api'),
    emitter = new EventEmitter()
}: ElasticsearchApiArgs): Promise<BaseReaderAPI> {
    return new BaseReaderAPI(config, client, emitter, logger);
}
