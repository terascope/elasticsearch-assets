import type {
    IndicesGetSettingsParams,
    SearchParams
} from 'elasticsearch';
import {
    Logger, TSError, get, AnyObject, withoutNil, DataEntity
} from '@terascope/utils';
import { DataTypeConfig } from '@terascope/data-types';
import got, { OptionsOfJSONResponseBody } from 'got';
import { DataFrame } from '@terascope/data-mate';
import { SpacesAPIConfig } from '../interfaces';
import { ReaderClient, SettingResults } from '../reader-client';

// eslint-disable-next-line
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

export default class SpacesReaderClient implements ReaderClient {
    // NOTE: currently we are not supporting id based reader queries
    // NOTE: currently we do no have access to _type or _id of each doc
    readonly config: SpacesAPIConfig;
    logger: Logger;
    protected uri: string;
    private retry: number;

    constructor(config: SpacesAPIConfig, logger: Logger) {
        this.config = config;
        this.logger = logger;
        this.uri = `${config.endpoint}/${config.index}`;
        this.retry = config.retry ?? 0;
    }

    getRequestOptions(
        query: AnyObject,
        format?: 'json'|'dfjson'
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
            timeout: this.config.timeout,
            retry: {
                limit: this.retry,
                methods: ['POST', 'GET'],
            },
            headers: this.config.headers || {},
        };
    }

    protected async makeRequest(
        query: AnyObject,
        format?: 'json'
    ): Promise<SearchResult>
    protected async makeRequest(
        query: AnyObject,
        format: 'dfjson'
    ): Promise<string>
    protected async makeRequest(
        query: AnyObject,
        format?: 'json'|'dfjson'
    ): Promise<SearchResult|string> {
        try {
            const { body } = await got.post<SearchResult>(
                this.uri, this.getRequestOptions(query, format)
            );
            return body;
        } catch (err) {
            if (err instanceof got.TimeoutError) {
                throw new TSError('HTTP request timed out connecting to API endpoint.', {
                    statusCode: 408,
                    context: {
                        endpoint: this.uri,
                        query,
                    }
                });
            }
            throw new TSError(err, {
                reason: 'Failure making search request',
                context: {
                    endpoint: this.uri,
                    query,
                }
            });
        }
    }

    protected translateSearchQuery(queryConfig: SearchParams): AnyObject {
        const { config } = this;

        const fields = get(queryConfig, '_source', null) as string[]|null;

        const dateFieldName = this.config.date_field_name;
        // put in the dateFieldName into fields so date reader can work
        if (fields && !fields.includes(dateFieldName)) {
            fields.push(dateFieldName);
        }

        const fieldsQuery = fields ? { fields: fields.join(',') } : {};
        const mustQuery = get(queryConfig, 'body.query.bool.must', null);

        function parseQueryConfig(mustArray: null | any[]): AnyObject {
            const queryOptions = {
                query_string: _parseEsQ,
                range: _parseDate,
                wildcard: _parseWildCard
            };
            const sortQuery: any = {};
            const geoQuery = _parseGeoQuery();
            let luceneQuery = '';

            if (mustArray) {
                mustArray.forEach((queryAction) => {
                    for (const [key, qConfig] of Object.entries(queryAction)) {
                        const queryFn = queryOptions[key];
                        if (queryFn) {
                            let queryStr = queryFn(qConfig);
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
                        sortQuery.sort = `${dateFieldName}:${queryConfig.body.sort[0][dateFieldName].order}`;
                    }
                });
            }

            let { size } = queryConfig;

            if (size == null) {
                ({ size } = config);
            }

            return Object.assign({}, geoQuery, sortQuery, fieldsQuery, {
                token: config.token,
                q: luceneQuery,
                size,
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
                ({ range } = queryConfig.body.query);
            }

            const dateStart = new Date(range[dateFieldName].gte);
            const dateEnd = new Date(range[dateFieldName].lt);

            // Teraslice date ranges are >= start and < end.
            return `${dateFieldName}:[${dateStart.toISOString()} TO ${dateEnd.toISOString()}}`;
        }

        return parseQueryConfig(mustQuery);
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
        query: SearchParams,
        useDataFrames: false,
        typeConfig?: DataTypeConfig
    ): Promise<DataEntity[]>;
    search(
        query: SearchParams,
        useDataFrames: true,
        typeConfig: DataTypeConfig
    ): Promise<DataFrame>;
    async search(
        query: SearchParams,
        useDataFrames: boolean,
    ): Promise<DataEntity[]|DataFrame> {
        if (!useDataFrames) {
            return this._searchRequest(query, false);
        }

        const start = Date.now();
        const data = await this._searchRequest(
            query, true, 'dfjson'
        );

        const dataFrame = await DataFrame.deserialize(data);

        const searchEnd = Date.now();
        dataFrame.metadata.search_end_time = searchEnd;
        dataFrame.metadata.metrics.search_time = searchEnd - start;
        dataFrame.metadata.metrics.fetched = dataFrame.metadata.metrics.returning;
        delete dataFrame.metadata.metrics.returning;

        // we do not have access to complexity right now
        return dataFrame;
    }

    _searchRequest(query: SearchParams, fullResponse: false): Promise<DataEntity[]>;
    _searchRequest(
        query: SearchParams,
        fullResponse: true,
        format: 'dfjson'
    ): Promise<string>;
    _searchRequest(
        query: SearchParams,
        fullResponse: true,
        format?: 'json'|'dfjson'
    ): Promise<SearchResult>;
    async _searchRequest(
        query: SearchParams,
        fullResponse?: boolean,
        format?: 'json'|'dfjson'
    ): Promise<DataEntity[]|SearchResult|string> {
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

    async count(queryConfig: SearchParams): Promise<number> {
        queryConfig.size = 0;
        const spaceResults = await this._searchRequest(queryConfig, true);
        return spaceResults.total;
    }

    /**
     * @todo this should verify the endpoint is valid
    */
    async verify(): Promise<void> {}

    getESVersion(): number {
        return 6;
    }

    async getSettings(_query: IndicesGetSettingsParams): Promise<SettingResults> {
        const { index, endpoint, token } = this.config;
        const uri = `${endpoint}/${index}/_info`;

        try {
            const { body: { params: { size: { max } } } } = await got(uri, {
                searchParams: { token },
                responseType: 'json',
                timeout: 1000000,
                retry: {
                    limit: this.retry,
                    methods: ['POST', 'GET'],
                }
            });

            return {
                [index]: {
                    settings: {
                        'index.max_result_window': max
                    },
                    defaults: {}
                }
            };
        } catch (err) {
            if (err instanceof got.TimeoutError) {
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
