import type { Client, SearchResponse } from 'elasticsearch';
import {
    Logger, TSError, get, AnyObject
} from '@terascope/utils';
import { DataTypeConfig } from '@terascope/data-types';
import got from 'got';
import { SpacesAPIConfig } from '../interfaces';

// eslint-disable-next-line
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

export default class SpacesClient {
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

    async makeRequest(query: AnyObject): Promise<SearchResult> {
        const { retry } = this;

        try {
            const { body } = await got<SearchResult>(this.uri, {
                searchParams: query,
                responseType: 'json',
                timeout: this.config.timeout,
                retry,
                headers: this.config.headers || {},
            });

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

    async apiSearch(queryConfig: AnyObject): Promise<AnyObject> {
        const { config } = this;
        const fields = get(queryConfig, '_source', null);
        const dateFieldName = this.config.date_field_name;
        // put in the dateFieldName into fields so date reader can work
        if (fields && !fields.includes(dateFieldName)) fields.push(dateFieldName);
        const fieldsQuery = fields ? { fields: fields.join(',') } : {};
        const mustQuery = get(queryConfig, 'body.query.bool.must', null);

        function parseQueryConfig(mustArray: null | any[]) {
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
            // geo sort will be taken care of in the teraserver search api
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

        function _parseGeoQuery() {
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

        function _parseEsQ(op?: any) {
            const { q } = queryConfig;
            const results = q || get(op, 'query', '');
            return results;
        }

        function _parseWildCard(op: Record<string, string>) {
            let str = '';

            for (const [key, value] of Object.entries(op)) {
                str += `${key}:${value}`;
            }

            return str;
        }

        function _parseDate(op: any) {
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

        const query = parseQueryConfig(mustQuery);

        try {
            return this.makeRequest(query);
        } catch (err) {
            return Promise.reject(new TSError(err, { reason: `error while calling endpoint ${this.uri}` }));
        }
    }

    private _makeESCompatible(response: AnyObject): SearchResponse<any> {
        let esResults: any[] = [];

        if (response.results) {
            esResults = response.results.map((result: any) => ({
                _source: result
            }));
        }

        return {
            hits: {
                hits: esResults,
                total: response.total
            },
            timed_out: false,
            _shards: {
                total: 1,
                successful: 1,
                failed: 0
            },
        } as SearchResponse<any>;
    }

    async getDataType(): Promise<DataTypeConfig> {
        const query = {
            token: this.config.token,
            q: '_exists_:_key',
            size: 0,
            include_type_config: true
        };

        const spaceResults = await this.makeRequest(query) as AnyObject;

        return spaceResults.type_config as DataTypeConfig;
    }

    async search(queryConfig: AnyObject): Promise<SearchResponse<any>> {
        const spaceResults = await this.apiSearch(queryConfig);
        return this._makeESCompatible(spaceResults);
    }

    async count(queryConfig: AnyObject): Promise<SearchResponse<any>> {
        queryConfig.size = 0;
        const spaceResults = await this.apiSearch(queryConfig);
        return this._makeESCompatible(spaceResults);
    }

    async version(): Promise<void> {}

    get cluster(): Partial<Client['cluster']> {
        const { index } = this.config;
        return {
            async stats() {
                return new Promise(((resolve) => {
                    resolve({
                        nodes: {
                            versions: ['0.5']
                        }
                    });
                }));
            },
            async getSettings() {
                return new Promise(((resolve) => {
                    const result = {};

                    result[index] = {
                        settings: {
                            index: {
                                max_result_window: 100000
                            }
                        }
                    };

                    resolve(result);
                }));
            }
        };
    }

    get indices(): Partial<Client['indices']> {
        const { index, endpoint, token } = this.config;
        return {
            async getSettings() {
                const uri = `${endpoint}/${index}/_info`;

                try {
                    const { body: { params: { size: { max } } } } = await got(uri, {
                        searchParams: { token },
                        responseType: 'json',
                        timeout: 1000000,
                        retry: 0
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
        };
    }
}

type SearchResult = {
    info: string;
    total: number;
    returning: number;
    results: any[];
};
