import { AnyObject, GeoPoint } from '@terascope/types';
import { parseGeoPoint } from '@terascope/utils';
import { SearchParams } from 'elasticsearch';
import { ESReaderOptions, SlicerDateResults } from './interfaces';

/**
 * Build the elasticsearch DSL query
 *
 * @todo this should be switch to return an xLucene query
*/
export function buildQuery(
    opConfig: ESReaderOptions, slice: Partial<SlicerDateResults>
): SearchParams {
    const query: SearchParams = {
        index: opConfig.index,
        size: slice.count,
        body: _buildRangeQuery(opConfig, slice),
    };

    if (opConfig.fields) query._source = opConfig.fields;

    return query;
}

function _buildRangeQuery(opConfig: ESReaderOptions, slice: Partial<SlicerDateResults>) {
    const body: AnyObject = {
        query: {
            bool: {
                must: [],
            },
        },
    };
    // is a range type query
    if (slice.start && slice.end) {
        const dateObj = {};
        const { date_field_name: dateFieldName } = opConfig;
        dateObj[dateFieldName] = {
            gte: slice.start,
            lt: slice.end,
        };

        body.query.bool.must.push({ range: dateObj });
    }
    // elasticsearch _id based query, we keep for v5 and lower
    if (slice.key) {
        body.query.bool.must.push({ wildcard: { _uid: slice.key } });
    }

    // this is used in the _id reader and the elasticsearch >= 6
    if (slice.wildcard) {
        const { field, value } = slice.wildcard;
        body.query.bool.must.push({ wildcard: { [field]: value } });
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
        geo_sort_unit: geoSortUnit,
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

export function geoSearch(opConfig: ESReaderOptions): AnyObject {
    let isGeoSort = false;
    const queryResults: AnyObject = {};
    // check for key existence to see if they are user defined
    if (opConfig.geo_sort_order || opConfig.geo_sort_unit || opConfig.geo_sort_point) {
        isGeoSort = true;
    }

    const {
        geo_box_top_left: geoBoxTopLeft,
        geo_box_bottom_right: geoBoxBottomRight,
        geo_point: geoPoint,
        geo_distance: geoDistance,
        geo_sort_point: geoSortPoint,
        geo_sort_order: geoSortOrder = 'asc',
        geo_sort_unit: geoSortUnit = 'm',
    } = opConfig;

    function createGeoSortQuery(location: GeoPoint) {
        const sortedSearch: AnyObject = { _geo_distance: {} };
        sortedSearch._geo_distance[opConfig.geo_field as string] = {
            lat: location[0],
            lon: location[1],
        };
        sortedSearch._geo_distance.order = geoSortOrder;
        sortedSearch._geo_distance.unit = geoSortUnit;
        return sortedSearch;
    }

    let parsedGeoSortPoint;

    if (geoSortPoint) {
        parsedGeoSortPoint = parseGeoPoint(geoSortPoint);
    }

    // Handle an Geo Bounding Box query
    if (geoBoxTopLeft) {
        const topLeft = parseGeoPoint(geoBoxTopLeft);
        const bottomRight = parseGeoPoint(geoBoxBottomRight as string);

        const searchQuery = {
            geo_bounding_box: {},
        };

        searchQuery.geo_bounding_box[opConfig.geo_field as string] = {
            top_left: {
                lat: topLeft[0],
                lon: topLeft[1],
            },
            bottom_right: {
                lat: bottomRight[0],
                lon: bottomRight[1],
            },
        };

        queryResults.query = searchQuery;

        if (isGeoSort) {
            queryResults.sort = createGeoSortQuery(parsedGeoSortPoint as GeoPoint);
        }

        return queryResults;
    }

    if (geoDistance) {
        const location = parseGeoPoint(geoPoint as string);
        const searchQuery = {
            geo_distance: {
                distance: geoDistance,
            },
        };

        searchQuery.geo_distance[opConfig.geo_field as string] = {
            lat: location[0],
            lon: location[1],
        };

        queryResults.query = searchQuery;
        const locationPoints = parsedGeoSortPoint || location;
        queryResults.sort = createGeoSortQuery(locationPoints);
    }

    return queryResults;
}
