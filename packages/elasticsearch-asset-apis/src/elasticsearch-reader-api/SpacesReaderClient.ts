import tls from 'tls';
import {
    Logger, TSError, get, isNil,
    AnyObject, withoutNil, DataEntity,
    isBoolean, isKey,
} from '@terascope/utils';
import { ClientParams, ClientResponse, ElasticsearchDistribution } from '@terascope/types';
import { DataTypeConfig } from '@terascope/data-types';
import got, {
    OptionsOfJSONResponseBody, Response, TimeoutError, RequestError
} from 'got';
import { DataFrame } from '@terascope/data-mate';
import { inspect } from 'node:util';
import {
    SpacesAPIConfig, ReaderClient, FetchResponseType
} from './interfaces.js';
import { throwRequestError } from './throwRequestError.js';

export class SpacesReaderClient implements ReaderClient {
    // NOTE: currently we are not supporting id based reader queries
    // NOTE: currently we do no have access to _type or _id of each doc
    readonly config: SpacesAPIConfig;
    logger: Logger;
    protected uri: string;
    private retry: number;
    caCerts: string[];

    constructor(config: SpacesAPIConfig, logger: Logger) {
        this.config = config;
        this.logger = logger;
        this.uri = `${config.endpoint}/${config.index}`;
        this.retry = config.retry ?? 3;
        this.caCerts = this.createCertsArray();
    }

    createCertsArray(): string[] {
        const opCerts: string[] = [];
        if (this.config.caCertificate) {
            opCerts.push(this.config.caCertificate);
        }
        const allCerts: string[] = opCerts.concat(tls.rootCertificates);

        return allCerts;
    }

    getRequestOptions(
        query: AnyObject,
        format?: 'json' | 'dfjson'
    ): OptionsOfJSONResponseBody {
        const {
            token,
            include_type_config,
            ...queryParams
        } = query;

        const json = withoutNil({
            ...queryParams,
            variables: this.config.variables
        });
        const isJSONResponse = (!format || format === 'json');

        return {
            searchParams: withoutNil({
                token,
                include_type_config,
                format
            }),
            responseType: isJSONResponse ? 'json' : undefined,
            json,
            throwHttpErrors: false,
            timeout: {
                request: this.config.timeout,
            },
            retry: {
                limit: this.retry,
                methods: ['POST', 'GET'],
            },
            headers: this.config.headers || {},
            https: { certificateAuthority: this.caCerts }
        };
    }

    protected async makeRequest(
        query: AnyObject,
        format?: 'json'
    ): Promise<SearchResult>;
    protected async makeRequest(
        query: AnyObject,
        format: 'dfjson'
    ): Promise<Buffer>;
    protected async makeRequest(
        query: AnyObject,
        format?: 'json' | 'dfjson'
    ): Promise<SearchResult | Buffer> {
        let response: Response<SearchResult | Buffer>;

        try {
            response = await got.post<SearchResult>(
                this.uri, this.getRequestOptions(query, format)
            );
        } catch (err) {
            if (err instanceof TimeoutError) {
                throw new TSError('HTTP request timed out connecting to API endpoint.', {
                    statusCode: 408,
                    context: {
                        endpoint: this.uri,
                        query,
                    }
                });
            }

            if (err instanceof RequestError && err.response) {
                throwRequestError(
                    this.uri,
                    err.response?.statusCode ?? 500,
                    err.response?.body
                );
            }

            throw new TSError(err, {
                reason: 'Failure making search request',
                context: {
                    endpoint: this.uri,
                    query,
                }
            });
        }
        if (response.statusCode >= 400) {
            throwRequestError(this.uri, response.statusCode, response.body);
        }
        if (format === 'dfjson') return response.rawBody;
        return response.body;
    }

    protected translateSearchQuery(queryConfig: ClientParams.SearchParams): AnyObject {
        const { config } = this;

        const size = queryConfig?.size ?? config.size;

        const fields = get(queryConfig, '_source', null) as string[] | null;

        const dateFieldName = this.config.date_field_name;
        // put in the dateFieldName into fields so date reader can work
        if (fields && !fields.includes(dateFieldName)) {
            fields.push(dateFieldName);
        }

        const fieldsQuery = fields ? { fields: fields.join(',') } : {};
        const mustQuery = get(queryConfig, 'body.query.bool.must', null);

        function parseQueryConfig(mustArray: null | any[], trackTotalHits?: any): AnyObject {
            const queryOptions: Record<string, (op: any) => string> = {
                query_string: _parseEsQ,
                range: _parseDate,
                wildcard: _parseWildCard,
                bool: _parseBoolQuery
            };
            const sortQuery: any = {};
            const geoQuery = _parseGeoQuery();
            let luceneQuery = '';

            if (mustArray) {
                mustArray.forEach((queryAction) => {
                    for (const [key, qConfig] of Object.entries(queryAction)) {
                        if (isKey(queryOptions, key) && queryOptions[key]) {
                            const queryFn = queryOptions[key];
                            let queryStr = queryFn(qConfig as Record<string, string>);
                            if (key !== 'range') queryStr = `(${queryStr})`;

                            if (luceneQuery.length) {
                                luceneQuery = `${luceneQuery} AND ${queryStr}`;
                            } else {
                                luceneQuery = queryStr;
                            }
                        }
                    }
                });
            } else {
                luceneQuery = _parseEsQ();
            }
            // geo sort will be taken care of in the spaces search api
            if (queryConfig.body && queryConfig.body.sort && queryConfig.body.sort.length > 0) {
                queryConfig.body.sort.forEach((sortType: any) => {
                    // We are checking for date sorts, geo sorts are handled by _parseGeoQuery
                    if (sortType[dateFieldName]) {
                        // there is only one sort allowed
                        // {"date":{"order":"asc"}}
                        sortQuery.sort = `${dateFieldName}:${queryConfig.body!.sort[0][dateFieldName].order}`;
                    }
                });
            }

            return Object.assign({}, geoQuery, sortQuery, fieldsQuery, {
                token: config.token,
                q: luceneQuery,
                size,
                track_total_hits: trackTotalHits
            });
        }

        function _parseGeoQuery(): AnyObject {
            const {
                geo_box_top_left: geoBoxTopLeft,
                geo_box_bottom_right: geoBoxBottomRight,
                geo_point: geoPoint,
                geo_distance: geoDistance,
                geo_sort_point: geoSortPoint,
                geo_sort_order: geoSortOrder,
                geo_sort_unit: geoSortUnit
            } = config;
            const geoQuery: any = {};
            if (geoBoxTopLeft) geoQuery.geo_box_top_left = geoBoxTopLeft;
            if (geoBoxBottomRight) geoQuery.geo_box_bottom_right = geoBoxBottomRight;
            if (geoPoint) geoQuery.geo_point = geoPoint;
            if (geoDistance) geoQuery.geo_distance = geoDistance;
            if (geoSortPoint) geoQuery.geo_sort_point = geoSortPoint;
            if (geoSortOrder) geoQuery.geo_sort_order = geoSortOrder;
            if (geoSortUnit) geoQuery.geo_sort_unit = geoSortUnit;
            return geoQuery;
        }

        function _parseEsQ(op?: any): string {
            const { q } = queryConfig;
            const results = q || get(op, 'query', '');
            return results;
        }

        function _parseWildCard(op: Record<string, string>): string {
            let str = '';

            for (const [key, value] of Object.entries(op)) {
                str += `${key}:${value}`;
            }

            return str;
        }

        function _parseDate(op: any): string {
            let range;
            if (op) {
                range = op;
            } else {
                ({ range } = queryConfig.body!.query);
            }

            const dateStart = new Date(range[dateFieldName].gte);
            const dateEnd = new Date(range[dateFieldName].lt);

            // Teraslice date ranges are >= start and < end.
            return `${dateFieldName}:[${dateStart.toISOString()} TO ${dateEnd.toISOString()}}`;
        }

        function _parseBoolQuery(op: any): string {
            if (!Array.isArray(op.should)) {
                throw new Error(`Invalid input to _parseBoolQuery ${inspect(op)}`);
            }
            const terms = op.should.map(({ wildcard }: any) => _parseWildCard(wildcard));
            return `(${terms.join(' OR ')})`;
        }

        let trackTotalHits: boolean | number = false;

        if (isBoolean(config.includeTotals)) {
            trackTotalHits = config.includeTotals;
        }
        if (config.includeTotals === 'number') {
            trackTotalHits = size + 1;
        }
        if (size === 0) {
            // in case client uses a search API instead of count API
            trackTotalHits = true;
        }

        return parseQueryConfig(mustQuery, trackTotalHits);
    }

    async getDataType(): Promise<DataTypeConfig> {
        const query = {
            token: this.config.token,
            q: '_exists_:_key',
            size: 0,
            include_type_config: true
        };

        const spaceResults = await this.makeRequest(query);
        return spaceResults.type_config!;
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
    ): Promise<DataEntity[] | DataFrame | Buffer> {
        if (responseType === FetchResponseType.data_entities) {
            return this._searchRequest(query, false);
        }

        const data = await this._searchRequest(
            query, true, 'dfjson'
        );

        if (responseType === FetchResponseType.raw) {
            return data;
        }

        return DataFrame.deserialize(data);
    }

    _searchRequest(query: ClientParams.SearchParams, fullResponse: false): Promise<DataEntity[]>;
    _searchRequest(
        query: ClientParams.SearchParams,
        fullResponse: true,
        format: 'dfjson'
    ): Promise<Buffer>;
    _searchRequest(
        query: ClientParams.SearchParams,
        fullResponse: true,
        format?: 'json' | 'dfjson'
    ): Promise<SearchResult>;
    async _searchRequest(
        query: ClientParams.SearchParams,
        fullResponse?: boolean,
        format?: 'json' | 'dfjson'
    ): Promise<DataEntity[] | SearchResult | Buffer> {
        const searchQuery = this.translateSearchQuery(query);
        if (fullResponse) {
            if (format === 'dfjson') {
                return this.makeRequest(searchQuery, format);
            }
            return this.makeRequest(searchQuery);
        }

        const result = await this.makeRequest(searchQuery);
        return result.results.map((record) => DataEntity.make(record, {
            // FIXME
        }));
    }

    async count(queryConfig: ClientParams.SearchParams): Promise<number> {
        queryConfig.size = 0;
        const spaceResults = await this._searchRequest(queryConfig, true);
        return spaceResults.total;
    }

    /**
     * @todo this should verify the endpoint is valid
    */
    async verify(): Promise<void> {}

    getESDistribution() {
        return this.config.clientMetadata?.distribution || ElasticsearchDistribution.elasticsearch;
    }

    async getSettings(_index: string): Promise<ClientResponse.IndicesGetSettingsResponse> {
        const { index, endpoint, token } = this.config;
        const uri = `${endpoint}/${index}/_info`;

        try {
            const response = await got<ClientResponse.IndicesGetSettingsResponse>(uri, {
                searchParams: { token },
                responseType: 'json',
                timeout: {
                    request: 1000000
                },
                retry: {
                    limit: this.retry,
                    methods: ['POST', 'GET'],
                },
                https: { certificateAuthority: this.caCerts }
            });
            const max = get(response, 'body.params.size.max', null) ?? get(response, 'params.size.max', null);

            if (isNil(max)) {
                throw new Error('Could not parse max from server response');
            }

            return {
                [index]: {
                    settings: {
                        'index.max_result_window': max
                    },
                    defaults: {}
                }
            };
        } catch (err) {
            if (err instanceof TimeoutError) {
                throw new TSError('HTTP request timed out connecting to API endpoint.', {
                    statusCode: 408,
                    context: {
                        endpoint: uri,
                    }
                });
            }
            throw new TSError(err, {
                reason: 'Failure making search request',
                context: {
                    endpoint: uri,
                }
            });
        }
    }
}

type SearchResult = {
    total: number;
    returning: number;
    results: any[];
    /**
     * If include_type_config is set,
     * we should get this back
    */
    type_config?: DataTypeConfig;
};
