import {
    Logger, TSError, get, AnyObject
} from '@terascope/job-components';
import got from 'got';
import { ApiConfig } from '../elasticsearch_reader/interfaces';

// eslint-disable-next-line
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

export default class ApiClient {
    // NOTE: currently we are not supporting id based reader queries
    // NOTE: currently we do no have access to _type or _id of each doc
    opConfig: ApiConfig;
    logger: Logger;

    constructor(opConfig: ApiConfig, logger: Logger) {
        this.opConfig = opConfig;
        this.logger = logger;
    }

    async makeRequest(uri: string, query: string): Promise<AnyObject> {
        try {
            const { body } = await got(uri, {
                query,
                json: true,
                timeout: this.opConfig.timeout,
                retry: 0
            });
            return body;
        } catch (err) {
            if (err instanceof got.TimeoutError) {
                throw new TSError('HTTP request timed out connecting to API endpoint.', {
                    statusCode: 408,
                    context: {
                        endpoint: uri,
                        query,
                    }
                });
            }
            throw new TSError(err, {
                reason: 'Failure making search request',
                context: {
                    endpoint: uri,
                    query,
                }
            });
        }
    }

    async apiSearch(queryConfig: any) {
        const { opConfig } = this;
        const fields = get(queryConfig, '_source', null);
        const dateFieldName = this.opConfig.date_field_name;
        // put in the dateFieldName into fields so date reader can work
        if (fields && !fields.includes(dateFieldName)) fields.push(dateFieldName);
        const fieldsQuery = fields ? { fields: fields.join(',') } : {};
        const mustQuery = get(queryConfig, 'body.query.bool.must', null);

        function parseQueryConfig(mustArray: null | any[]) {
            const queryOptions = {
                query_string: _parseEsQ,
                range: _parseDate,
            };
            const sortQuery: any = {};
            const geoQuery = _parseGeoQuery();
            let luceneQuery = '';

            if (mustArray) {
                mustArray.forEach((queryAction) => {
                    for (const [key, config] of Object.entries(queryAction)) {
                        const queryFn = queryOptions[key];
                        if (queryFn) {
                            let queryStr = queryFn(config);
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
                ({ size } = opConfig);
            }

            return Object.assign({}, geoQuery, sortQuery, fieldsQuery, {
                token: opConfig.token,
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
            } = opConfig;
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

        const uri = `${opConfig.endpoint}/${opConfig.index}`;
        const query = parseQueryConfig(mustQuery);

        try {
            const response = await this.makeRequest(uri, query);

            let esResults = [];
            if (response.results) {
                esResults = response.results.map((result: any) => ({ _source: result }));
            }

            return ({
                hits: {
                    hits: esResults,
                    total: response.total
                },
                timed_out: false,
                _shards: {
                    total: 1,
                    successful: 1,
                    failed: 0
                }
            });
        } catch (err) {
            return Promise.reject(new TSError(err, { reason: `error while calling endpoint ${uri}` }));
        }
    }

    search(queryConfig: any) {
        return this.apiSearch(queryConfig);
    }

    count(queryConfig: any) {
        queryConfig.size = 0;
        return this.apiSearch(queryConfig);
    }

    get cluster() {
        const { index } = this.opConfig;
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
}
