import elasticAPI from '@terascope/elasticsearch-api';
import { DataFrame } from '@terascope/data-mate';
import {
    AnyObject, DataEntity, get, Logger
} from '@terascope/utils';
import type {
    Client, SearchParams, SearchResponse, CountParams
} from 'elasticsearch';
import { DataTypeConfig } from '@terascope/types';
import { ReaderClient, FetchResponseType, SettingResults } from './interfaces';

export class ElasticsearchReaderClient implements ReaderClient {
    private readonly _baseClient: Client;
    private readonly client: elasticAPI.Client;
    private readonly fullResponseClient: elasticAPI.Client;

    constructor(
        elasticsearchClient: Client,
        clientConfig: {
            connection?: string
            index: string
        },
        logger: Logger,
    ) {
        this._baseClient = elasticsearchClient;
        this.client = elasticAPI(elasticsearchClient, logger, {
            ...clientConfig,
            full_response: false
        });
        this.fullResponseClient = elasticAPI(elasticsearchClient, logger, {
            ...clientConfig,
            full_response: true
        });
    }

    async count(query: SearchParams): Promise<number> {
        // this internally does a search request with size=0
        // I think the types a wrong
        return this.client.count(query as CountParams);
    }

    search(
        query: SearchParams,
        responseType: FetchResponseType.raw,
        typeConfig?: DataTypeConfig
    ): Promise<Buffer>;
    search(
        query: SearchParams,
        responseType: FetchResponseType.data_entities,
        typeConfig?: DataTypeConfig
    ): Promise<DataEntity[]>;
    search(
        query: SearchParams,
        responseType: FetchResponseType.data_frame,
        typeConfig: DataTypeConfig
    ): Promise<DataFrame>;
    async search(
        query: SearchParams,
        responseType: FetchResponseType,
        typeConfig?: DataTypeConfig
    ): Promise<DataEntity[]|DataFrame|Buffer> {
        if (responseType === FetchResponseType.data_entities) {
            return this._searchRequest(query, false);
        }

        const searchResults = await this._searchRequest(
            query, true
        );

        const records = searchResults.hits.hits.map((data) => data._source);
        const metrics = {
            total: get(searchResults, 'hits.total.value', get(searchResults, 'hits.total'))
        };

        // we do not have access to complexity right now
        const dataFrame = DataFrame.fromJSON(
            typeConfig!,
            records,
            {
                name: '<unknown>',
                metadata: {
                    metrics
                }
            }
        );
        if (responseType === FetchResponseType.raw) {
            return Buffer.from(dataFrame.serialize());
        }
        return dataFrame;
    }

    _searchRequest(query: SearchParams, fullResponse: false): Promise<DataEntity[]>;
    _searchRequest(query: SearchParams, fullResponse: true): Promise<SearchResponse<AnyObject>>;
    async _searchRequest(
        query: SearchParams,
        fullResponse?: boolean
    ): Promise<DataEntity[]|SearchResponse<AnyObject>> {
        if (fullResponse) {
            return this.fullResponseClient.search(query);
        }
        return this.client.search(query);
    }

    getESVersion(): number {
        return this.client.getESVersion();
    }

    async verify(): Promise<void> {
        // this is method in api is badly named
        await this.client.version();
    }

    async getSettings(index: string): Promise<SettingResults> {
        const results = await this._baseClient.indices.getSettings({
            index,
            flatSettings: true,
            includeDefaults: true,
            allowNoIndices: true
        });

        return results.body;
    }
}
