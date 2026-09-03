import {
    ClientParams, GeoDistanceSort, GeoDistanceUnit,
    GeoPoint, GeoQuery, SortOrder
} from '@terascope/types';
import { isString } from '@terascope/core-utils';
import { parseGeoPoint } from '@terascope/geo-utils';
import { ESReaderOptions, ReaderSlice } from './interfaces.js';

/**
 * Build the elasticsearch DSL query
*/
export function buildQuery(
    opConfig: ESReaderOptions, params: ReaderSlice
): ClientParams.SearchParams {
    if (params.count == null) {
        throw new Error('Expected count to buildQuery');
    }
    const query: ClientParams.SearchParams = {
        index: opConfig.index,
        size: params.count,
        body: _buildRangeQuery(opConfig, params),
    };

    if (opConfig.total_optimization) {
        let trackCount: number | boolean = false;

        if (params.count === 0) {
            if (opConfig.recurse_optimization) {
                trackCount = true;
            } else {
                trackCount = opConfig.size + 1;
            }
        }

        query.track_total_hits = trackCount;
    } else {
        query.track_total_hits = true;
    }

    if (opConfig.fields) {
        query._source = opConfig.fields;
    }

    return query;
}

function _buildRangeQuery(
    opConfig: ESReaderOptions, params: ReaderSlice
) {
    const body: Record<string, any> = {
        query: {
            bool: {
                must: [],
            },
        },
    };
    // is a range type query
    if (params.start && params.end) {
        const dateObj: Record<string, { gte: string; lt: string }> = {};
        const { date_field_name: dateFieldName } = opConfig;
        dateObj[dateFieldName] = {
            gte: params.start,
            lt: params.end,
        };

        body.query.bool.must.push({ range: dateObj });
    }
    // elasticsearch _id based query, we keep for v5 and lower
    if (params.keys?.length) {
        const idFieldName = opConfig.id_field_name;
        if (!isString(idFieldName)) {
            throw new Error('Missing id_field_name for id slicer');
        }

        if (opConfig.recurse_optimization) {
            body.query.bool.must.push({
                bool: {
                    should: params.keys.map((key) => {
                        return {
                            regexp: { [idFieldName]: `${key}.*` }
                        };
                    })
                }
            });
        } else {
            body.query.bool.must.push({
                bool: {
                    should: params.keys.map((key) => ({
                        wildcard: { [idFieldName]: `${key}*` }
                    }))
                }
            });
        }
    }

    // elasticsearch lucene based query
    if (opConfig.query) {
        body.query.bool.must.push({
            query_string: {
                query: opConfig.query,
            },
        });
    }

    if (opConfig.geo_field) {
        validateGeoParameters(opConfig);
        const geoQuery = geoSearch(opConfig);
        body.query.bool.must.push(geoQuery.query);
        if (geoQuery.sort) body.sort = [geoQuery.sort];
    }

    return body;
}

export function validateGeoParameters(opConfig: ESReaderOptions): void {
    const {
        geo_field: geoField,
        geo_box_top_left: geoBoxTopLeft,
        geo_box_bottom_right: geoBoxBottomRight,
        geo_point: geoPoint,
        geo_distance: geoDistance,
        geo_sort_point: geoSortPoint,
        geo_sort_order: geoSortOrder,
        geo_sort_unit: geoSortUnit
    } = opConfig;

    function isBoundingBoxQuery() {
        return geoBoxTopLeft && geoBoxBottomRight;
    }

    function isGeoDistanceQuery() {
        return geoPoint && geoDistance;
    }

    if (geoBoxTopLeft && geoPoint) {
        throw new Error('geo_box and geo_distance queries can not be combined.');
    }

    if ((geoPoint && !geoDistance) || (!geoPoint && geoDistance)) {
        throw new Error(
            'Both geo_point and geo_distance must be provided for a geo_point query.'
        );
    }

    if ((geoBoxTopLeft && !geoBoxBottomRight) || (!geoBoxTopLeft && geoBoxBottomRight)) {
        throw new Error(
            'Both geo_box_top_left and geo_box_bottom_right must be provided for a geo bounding box query.'
        );
    }

    if (geoBoxTopLeft && (geoSortOrder || geoSortUnit) && !geoSortPoint) {
        throw new Error(
            'bounding box search requires geo_sort_point to be set if any other geo_sort_* parameter is provided'
        );
    }

    if ((geoBoxTopLeft || geoPoint || geoDistance || geoSortPoint) && !geoField) {
        throw new Error(
            'geo box search requires geo_field to be set if any other geo query parameters are provided'
        );
    }

    if (geoField && !(isBoundingBoxQuery() || isGeoDistanceQuery())) {
        throw new Error(
            'if geo_field is specified then the appropriate geo_box or geo_distance query parameters need to be provided as well'
        );
    }
}

export function geoSearch(opConfig: ESReaderOptions): Record<string, any> {
    const geoField = opConfig.geo_field;
    if (!geoField) throw new Error('Missing geo_field for geo search');

    const queryResults: { query?: GeoQuery; sort?: GeoDistanceSort } = {};

    const createGeoDistanceSort = (location: GeoPoint): GeoDistanceSort => {
        return {
            _geo_distance: {
                order: (opConfig.geo_sort_order as SortOrder) || 'asc',
                unit: (opConfig.geo_sort_unit || 'm') as GeoDistanceUnit,
                [opConfig.geo_sort_field || geoField]: {
                    lat: location.lat,
                    lon: location.lon
                }
            }
        };
    };

    let parsedGeoSortPoint: GeoPoint | undefined;
    if (opConfig.geo_sort_point) {
        parsedGeoSortPoint = parseGeoPoint(opConfig.geo_sort_point);
    }

    // Geo Bounding Box query
    if (opConfig.geo_box_top_left) {
        const topLeft = parseGeoPoint(opConfig.geo_box_top_left);
        const bottomRight = parseGeoPoint(opConfig.geo_box_bottom_right);

        queryResults.query = {
            geo_bounding_box: {
                [geoField]: {
                    top_left: {
                        lat: topLeft.lat,
                        lon: topLeft.lon
                    },
                    bottom_right: {
                        lat: bottomRight.lat,
                        lon: bottomRight.lon
                    }
                }
            }
        };

        const isGeoSort = Boolean(
            opConfig.geo_sort_order || opConfig.geo_sort_unit
            || opConfig.geo_sort_point || opConfig.geo_sort_field
        );
        if (isGeoSort && parsedGeoSortPoint) {
            queryResults.sort = createGeoDistanceSort(parsedGeoSortPoint);
        }

        return queryResults;
    }

    // Geo Distance query
    if (opConfig.geo_distance) {
        const location = parseGeoPoint(opConfig.geo_point);
        queryResults.query = {
            geo_distance: {
                distance: opConfig.geo_distance,
                [geoField]: {
                    lat: location.lat,
                    lon: location.lon
                }
            },
        };

        const locationPoints = parsedGeoSortPoint || location;
        queryResults.sort = createGeoDistanceSort(locationPoints);
    }

    return queryResults;
}
