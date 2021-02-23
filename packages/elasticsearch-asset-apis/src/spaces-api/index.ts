import { debugLogger, Logger } from '@terascope/utils';
import { EventEmitter } from 'events';
import SpacesClient from './spaces-client';
import { BaseReaderAPI } from '../base-api';
import { SpacesAPIConfig } from '../interfaces';

interface SpacesAPIArgs {
    config: SpacesAPIConfig,
    logger?: Logger;
    emitter?: EventEmitter;
}

export async function createSpacesAPI({
    config,
    logger = debugLogger('spaces-api'),
    emitter = new EventEmitter()
}: SpacesAPIArgs): Promise<BaseReaderAPI> {
    if (config.use_data_frames) {
        config.full_response = true;
    }

    const client = new SpacesClient(config, logger);

    if (config.use_data_frames && !config.type_config) {
        config.type_config = await client.getDataType();
    }

    const api = new BaseReaderAPI(config, client, emitter, logger);

    await api.validateSize();

    return api;
}
