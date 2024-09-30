import elasticAPI from '@terascope/elasticsearch-api';
import { Client } from 'elasticsearch-store';
import { DataFrame } from '@terascope/data-mate';
import {
    AnyObject, DataEntity, get, Logger
} from '@terascope/utils';
import { DataTypeConfig, ClientParams, ClientResponse } from '@terascope/types';
import { ReaderClient, FetchResponseType } from './interfaces.js';

export class ElasticsearchReaderClient implements ReaderClient {
    private readonly _baseClient: Client;
    private readonly client: elasticAPI.Client;
    private readonly fullResponseClient: elasticAPI.Client;

    constructor(
        elasticsearchClient: Client,
        clientConfig: {
            connection?: string;
            index: string;
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

    async count(query: ClientParams.SearchParams): Promise<number> {
        // this internally does a search request with size=0
        // I think the types a wrong
        return this.client.count(query as any);
    }

    search(
        query: ClientParams.SearchParams,
        responseType: FetchResponseType.raw,
        typeConfig?: DataTypeConfig
    ): Promise<Buffer>;
    search(
        query: ClientParams.SearchParams,
        responseType: FetchResponseType.data_entities,
        typeConfig?: DataTypeConfig
    ): Promise<DataEntity[]>;
    search(
        query: ClientParams.SearchParams,
        responseType: FetchResponseType.data_frame,
        typeConfig: DataTypeConfig
    ): Promise<DataFrame>;
    async search(
        query: ClientParams.SearchParams,
        responseType: FetchResponseType,
        typeConfig?: DataTypeConfig
    ): Promise<DataEntity[] | DataFrame | Buffer> {
        if (responseType === FetchResponseType.data_entities) {
            return this._searchRequest(query, false);
        }

        const searchResults = await this._searchRequest(
            query, true
        );

        const records = searchResults.hits.hits.map((data) => data._source as AnyObject);
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

    _searchRequest(query: ClientParams.SearchParams, fullResponse: false): Promise<DataEntity[]>;
    _searchRequest(
        query: ClientParams.SearchParams,
        fullResponse: true
    ): Promise<ClientResponse.SearchResponse<AnyObject>>;
    async _searchRequest(
        query: ClientParams.SearchParams,
        fullResponse?: boolean
    ): Promise<DataEntity[] | ClientResponse.SearchResponse<AnyObject>> {
        if (fullResponse) {
            return this.fullResponseClient.search(
                query as any
            ) as Promise<ClientResponse.SearchResponse<AnyObject>>;
        }
        return this.client.search(query as any) as Promise<DataEntity[]>;
    }

    getESVersion(): number {
        return this.client.getESVersion();
    }

    async verify(): Promise<void> {
        // this is method in api is badly named
        await this.client.version();
    }

    async getSettings(
        index: string
    ): Promise<ClientResponse.IndicesGetSettingsResponse> {
        return this._baseClient.indices.getSettings({
            index,
            flat_settings: true,
            include_defaults: true,
            allow_no_indices: true,
        });
    }
}
