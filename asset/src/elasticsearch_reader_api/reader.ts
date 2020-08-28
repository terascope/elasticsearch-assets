import elasticAPI from '@terascope/elasticsearch-api';
import {
    AnyObject,
    DataEntity,
    isObjectEntity,
    getTypeOf,
    Logger,
} from '@terascope/job-components';
import { CountParams } from 'elasticsearch';
import { buildQuery } from '../elasticsearch_reader/elasticsearch_date_range/helpers';
import { ESReaderOptions, SlicerDateResults } from '../elasticsearch_reader/interfaces';

export default class APIReader {
    readonly config: ESReaderOptions;
    logger: Logger;
    _baseClient: AnyObject;
    client: elasticAPI.Client;
    private hasDefaultQueries = false;
    private windowSize: undefined | number = undefined;

    constructor(config: ESReaderOptions, client: AnyObject, logger: Logger) {
        const { connection, index } = config;
        const clientConfig = {
            full_response: false,
            connection,
            index
        };

        this.config = Object.freeze(config);
        this.logger = logger;
        this._baseClient = client;
        this.client = elasticAPI(client, logger, clientConfig);
        if (config.query || config.geo_field) this.hasDefaultQueries = true;
    }

    private validate(query: unknown) {
        if (isObject(query)) {
            if (
                !(query.start || query.end)
                && !(query.key || query.wildcard)
                && !this.hasDefaultQueries
            ) {
                throw new Error('No valid query parameters, it must have start/end, or key/wildcard or apiConfig query or geo_field set');
            }
        } else {
            throw new Error(`Invalid query parameters, must receive an object, got ${getTypeOf(query)}`);
        }
    }

    async count(queryParams: Partial<SlicerDateResults>): Promise<number> {
        this.validate(queryParams);
        const query = buildQuery(this.config, queryParams);
        return this.client.count(query as CountParams);
    }

    async fetch(queryParams: Partial<SlicerDateResults>): Promise<DataEntity[]> {
        this.validate(queryParams);
        // attempt to get window if not set
        if (!this.windowSize) await this.getWindowSize();
        // if we did go ahead and complete query
        if (this.windowSize) {
            const query = buildQuery(this.config, queryParams);
            query.size = this.windowSize;
            return this.client.search(query);
        }
        // index is not up, return empty, we log in getWindowSize
        return [];
    }

    async getWindowSize(): Promise<void> {
        const window = 'index.max_result_window';
        const { index } = this.config;

        const query = {
            index,
            flat_settings: true,
            include_defaults: true,
            allow_no_indices: true
        };

        try {
            const settings = await this._baseClient.indices.getSettings(query);
            const defaultPath = settings[index].defaults[window];
            const configPath = settings[index].settings[window];

            if (defaultPath) this.windowSize = defaultPath;
            if (configPath) this.windowSize = configPath;
        } catch (_err) {
            this.logger.warn(`index: ${this.config.index} is not yet created`);
        }
    }

    get version(): number {
        return this.client.getESVersion();
    }
}

function isObject(val: unknown): val is AnyObject {
    return isObjectEntity(val);
}
