import { debugLogger, Logger } from '@terascope/utils';
import { EventEmitter } from 'events';
import SpacesReaderClient from './spaces-client';
import { ElasticsearchReaderAPI } from '../elasticsearch-reader-api';
import { SpacesAPIConfig } from '../interfaces';

interface SpacesAPIArgs {
    config: SpacesAPIConfig,
    logger?: Logger;
    emitter?: EventEmitter;
}

export { SpacesReaderClient };

export async function createSpacesAPI({
    config,
    logger = debugLogger('spaces-api'),
    emitter = new EventEmitter()
}: SpacesAPIArgs): Promise<ElasticsearchReaderAPI> {
    const client = new SpacesReaderClient(config, logger);

    if (config.use_data_frames && !config.type_config) {
        config.type_config = await client.getDataType();
    }

    return new ElasticsearchReaderAPI(config, client, emitter, logger);
}
