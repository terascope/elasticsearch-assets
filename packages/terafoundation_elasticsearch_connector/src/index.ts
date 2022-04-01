import { Logger, pDefer } from '@terascope/utils';
import type { Client, ConfigOptions } from 'elasticsearch';
import { legacySchema } from './schema';
import { logWrapper } from './log-wrapper';
import { createClient } from './create-client';

function create(customConfig: ConfigOptions, logger: Logger): {
    client: Client,
    log: any;
} {
    const elasticsearch = require('elasticsearch');

    logger.info(`using elasticsearch hosts: ${customConfig.host}`);

    customConfig.defer = pDefer;

    const client = new elasticsearch.Client(customConfig);

    return {
        client,
        log: logWrapper(logger)
    };
}

export default {
    create,
    createClient,
    config_schema(): Record<string, any> {
        return legacySchema;
    }
};
