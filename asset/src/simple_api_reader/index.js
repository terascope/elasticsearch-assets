'use strict';

const got = require('got');
const _ = require('lodash');
const Promise = require('bluebird');
const { getOpConfig, TSError } = require('@terascope/job-components');
const slicerFn = require('../elasticsearch_reader/elasticsearch_date_range/slicer');
const readerFn = require('../elasticsearch_reader/reader');
const reader = require('../elasticsearch_reader');

// eslint-disable-next-line
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const MODULE_NAME = 'simple_api_reader';

function createClient(context, opConfig) {
    // NOTE: currently we are not supporting id based reader queries
    // NOTE: currently we do no have access to _type or _id of each doc
    const { logger } = context;

    async function makeRequest(uri, query) {
        try {
            const { body } = await got(uri, {
                query,
                json: true,
                timeout: opConfig.timeout,
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

    function apiSearch(queryConfig) {
        const fields = _.get(queryConfig, '_source', null);
        const dateFieldName = opConfig.date_field_name;
        // put in the dateFieldName into fields so date reader can work
        if (fields && !fields.includes(dateFieldName)) fields.push(dateFieldName);
        const fieldsQuery = fields ? { fields: fields.join(',') } : {};
        const mustQuery = _.get(queryConfig, 'body.query.bool.must', null);

        function parseQueryConfig(mustArray) {
            const queryOptions = {
                query_string: _parseEsQ,
                range: _parseDate,
            };
            const sortQuery = {};
            const geoQuery = _parseGeoQuery();
            let luceneQuery = '';

            if (mustArray) {
                mustArray.forEach((queryAction) => {
                    _.forOwn(queryAction, (config, key) => {
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
                    });
                });
            } else {
                luceneQuery = _parseEsQ();
            }
            // geo sort will be taken care of in the teraserver search api
            if (queryConfig.body && queryConfig.body.sort && queryConfig.body.sort.length > 0) {
                queryConfig.body.sort.forEach((sortType) => {
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
            const geoQuery = {};
            if (geoBoxTopLeft) geoQuery.geo_box_top_left = geoBoxTopLeft;
            if (geoBoxBottomRight) geoQuery.geo_box_bottom_right = geoBoxBottomRight;
            if (geoPoint) geoQuery.geo_point = geoPoint;
            if (geoDistance) geoQuery.geo_distance = geoDistance;
            if (geoSortPoint) geoQuery.geo_sort_point = geoSortPoint;
            if (geoSortOrder) geoQuery.geo_sort_order = geoSortOrder;
            if (geoSortUnit) geoQuery.geo_sort_unit = geoSortUnit;
            return geoQuery;
        }

        function _parseEsQ(op) {
            const { q } = queryConfig;
            const results = q || _.get(op, 'query', '');
            return results;
        }

        function _parseDate(op) {
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

        function callTeraserver() {
            const uri = `${opConfig.endpoint}/${opConfig.index}`;
            const query = parseQueryConfig(mustQuery);

            return Promise.resolve()
                .then(() => makeRequest(uri, query))
                .then((response) => {
                    let esResults = [];
                    if (response.results) {
                        esResults = _.map(response.results, (result) => ({ _source: result }));
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
                })
                .catch((err) => {
                    logger.error(`error while calling endpoint ${uri}, error: ${err.message}`);
                    return Promise.reject(err);
                });
        }

        return callTeraserver();
    }

    return {
        search(queryConfig) {
            return apiSearch(queryConfig);
        },
        count(queryConfig) {
            queryConfig.size = 0;

            return apiSearch(queryConfig);
        },
        cluster: {
            stats() {
                return new Promise(((resolve) => {
                    resolve({
                        nodes: {
                            versions: ['0.5']
                        }
                    });
                }));
            },
            getSettings() {
                return new Promise(((resolve) => {
                    const result = {};

                    result[opConfig.index] = {
                        settings: {
                            index: {
                                max_result_window: 100000
                            }
                        }
                    };

                    resolve(result);
                }));
            }
        }
    };
}

function newSlicer(context, executionContext, retryData, logger) {
    const opConfig = getOpConfig(executionContext.config, MODULE_NAME);
    const client = createClient(context, opConfig);
    return slicerFn(context, opConfig, executionContext, retryData, logger, client);
}

function newReader(context, opConfig) {
    const client = createClient(context, opConfig);
    return readerFn(opConfig, client);
}

function schema() {
    const esSchema = reader.schema();
    const apiSchema = {
        endpoint: {
            doc: 'The base API endpoint to read from: i.e. http://yourdomain.com/api/v1',
            default: '',
            format: 'required_String'
        },
        token: {
            doc: 'API access token for making requests',
            default: '',
            format: 'required_String'
        },
        timeout: {
            doc: 'Time in milliseconds to wait for a connection to timeout.',
            default: 300000
        },
    };

    return _.assign({}, esSchema, apiSchema);
}

module.exports = {
    newReader,
    newSlicer,
    schema,
    createClient
};
