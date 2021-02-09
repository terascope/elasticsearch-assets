import elasticAPI from '@terascope/elasticsearch-api';
import { isNil, isPlainObject, isString } from '@terascope/utils';
import { ElasticsearchSender } from './bulk-sender';
import { ElasticsearchSenderConfig } from './interfaces';

export * from './interfaces';

interface BulkAPIArgs {
    config: ElasticsearchSenderConfig,
    client: elasticAPI.Client;
}

function validateConfig(input: unknown): ElasticsearchSenderConfig {
    if (!isPlainObject(input)) throw new Error('Bulk Sender API config must be an object');
    const config = {
        ...input as Record<string, any>
    } as Partial<ElasticsearchSenderConfig>;

    if (config.update_fields) {
        if (!Array.isArray(config.update_fields) || !config.update_fields.every(isString)) {
            throw new Error('Invalid parameter update_fields, it must be an array of strings');
        }
    }

    if (isNil(config.index) || !isString(config.index) || config.index.length === 0) {
        throw new Error(`Invalid index parameter: ${config.index}, it must be provided be be of types string`);
    }

    const actionSet = new Set();
    // only one of these should be set to true at a time
    ['delete', 'create', 'update', 'index', 'upsert'].forEach((key: string) => {
        if (config[key] === true) actionSet.add(key);
        if (actionSet.size > 1) {
            const actions = Array.from(actionSet).join(', ');
            const msg = `Invalid parameters, only one of "${actions}" may be set at a time`;
            throw new Error(msg);
        }
    });

    return input as ElasticsearchSenderConfig;
}

export function createBulkSenderAPI(
    { client, config: inputConfig }: BulkAPIArgs
): ElasticsearchSender {
    const config = validateConfig(inputConfig);
    return new ElasticsearchSender(client, config);
}
