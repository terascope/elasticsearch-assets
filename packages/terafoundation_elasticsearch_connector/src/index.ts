import { Logger, pDefer } from '@terascope/utils';
import type { Client, ConfigOptions } from 'elasticsearch';
import schema from './schema';

function logWrapper(logger: Logger) {
    return function _logger() {
        return {
            error: logger.error.bind(logger),
            warning: logger.warn.bind(logger),
            info: logger.info.bind(logger),
            debug: logger.debug.bind(logger),
            trace(
                method: any,
                requestUrl: any,
                body: any,
                responseBody: any,
                responseStatus: any
            ) {
                logger.trace({
                    method,
                    requestUrl,
                    body,
                    responseBody,
                    responseStatus
                });
            },
            close() {}
        };
    };
}

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
    config_schema(): Record<string, any> {
        return schema;
    }
};
