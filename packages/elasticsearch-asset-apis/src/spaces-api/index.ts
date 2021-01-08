import { debugLogger, Logger } from '@terascope/job-components';
import { EventEmitter } from 'events';
import SpacesClient from './spaces-client';
import BaseReaderAPI from '../base-api';
import { SpacesApiConfig } from '../interfaces';

interface SpacesApiArgs {
    config: SpacesApiConfig,
    logger?: Logger;
    emitter?: EventEmitter;
}

export default async function createSpacesApi({
    config,
    logger = debugLogger('spaces-api'),
    emitter = new EventEmitter()
}: SpacesApiArgs): Promise<BaseReaderAPI> {
    if (config.use_data_frames) {
        config.full_response = true;
    }

    const client = new SpacesClient(config, logger);

    if (config.use_data_frames && !config.type_config) {
        config.type_config = await client.getDataType();
    }

    return new BaseReaderAPI(config, client, emitter, logger);
}
