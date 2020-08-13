import {
    Logger, times, AnyObject, toNumber, parseGeoPoint,
} from '@terascope/job-components';
import moment from 'moment';
import fs from 'fs';
// @ts-expect-error
import dateMath from 'datemath-parser';
import { GeoPoint } from '@terascope/types';
import { SearchParams } from 'elasticsearch';
import {
    StartPointConfig,
    SlicerDateConfig,
    DateSegments,
    SlicerDateResults,
    ParsedInterval,
    DateConfig,
    ESReaderConfig,
    IDReaderSlice,
} from '../interfaces';
import { ESIDReaderConfig } from '../../id_reader/interfaces';

type ReaderConfig = ESReaderConfig | ESIDReaderConfig;
type BuildQueryInput = SlicerDateResults | IDReaderSlice;

export function dateOptions(value: string): moment.unitOfTime.Base {
    const options = {
        year: 'y',
        years: 'y',
        y: 'y',
        months: 'M',
        month: 'M',
        mo: 'M',
        mos: 'M',
        M: 'M',
        weeks: 'w',
        week: 'w',
        wks: 'w',
        wk: 'w',
        w: 'w',
        days: 'd',
        day: 'd',
        d: 'd',
        hours: 'h',
        hour: 'h',
        hr: 'h',
        hrs: 'h',
        h: 'h',
        minutes: 'm',
        minute: 'm',
        min: 'm',
        mins: 'm',
        m: 'm',
        seconds: 's',
        second: 's',
        s: 's',
        milliseconds: 'ms',
        millisecond: 'ms',
        ms: 'ms'
    };

    if (options[value]) {
        return options[value];
    }

    throw new Error(`the time descriptor of "${value}" for the interval is malformed`);
}

export function processInterval(
    interval: string,
    timeResolution: moment.unitOfTime.Base,
    esDates?: DateSegments
): ParsedInterval {
    // one or more digits, followed by one or more letters, case-insensitive
    const regex = /(\d+)(\w+)/i;
    const intervalMatch = regex.exec(interval);

    if (intervalMatch === null) {
        throw new Error('elasticsearch_reader interval and/or delay are incorrectly formatted. Needs to follow [number][letter\'s] format, e.g. "12s"');
    }

    // don't need first parameter, its the full string
    intervalMatch.shift();

    const newInterval: ParsedInterval = [toNumber(intervalMatch[0]), dateOptions(intervalMatch[1])];
    return compareInterval(newInterval, timeResolution, esDates);
}

function compareInterval(
    interval: ParsedInterval, timeResolution: string, esDates?: DateSegments,
): ParsedInterval {
    if (esDates) {
        const datesDiff = esDates.limit.diff(esDates.start);
        const intervalDiff = moment.duration(Number(interval[0]), interval[1] as moment.unitOfTime.Base).as('milliseconds');

        if (intervalDiff > datesDiff) {
            if (timeResolution === 's') {
                return [Math.ceil(datesDiff / 1000), 's'];
            }
            return [datesDiff, 'ms'];
        }
    }

    return interval;
}

// "2016-01-19T13:33:09.356-07:00"
export const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSSZ';

// 2016-06-29T12:44:57-07:00
export const dateFormatSeconds = 'YYYY-MM-DDTHH:mm:ssZ';

type RetryCb = (msg: any) => Promise<any>

// TODO: this might be broken, there is no msg
export function retryModule(logger: Logger, numOfRetries: number) {
    const retry = {};
    return (_key: string | AnyObject, err: Error, fn: RetryCb, msg: any) => {
        logger.error(err, 'error while getting next slice');
        const key = typeof _key === 'string' ? _key : JSON.stringify(_key);

        if (!retry[key]) {
            retry[key] = 1;
            return fn(msg);
        }

        retry[key] += 1;
        if (retry[key] > numOfRetries) {
            return Promise.reject(
                new Error(`max_retries met for slice, key: ${key}`)
            );
        }

        return fn(msg);
    };
}

export function existsSync(filename: string): boolean {
    try {
        fs.accessSync(filename);
        return true;
    } catch (ex) {
        return false;
    }
}

export function getMilliseconds(interval: any[]): number {
    const conversions = {
        d: 86400000,
        h: 3600000,
        m: 60000,
        s: 1000,
        ms: 1
    };

    return interval[0] * conversions[interval[1]];
}

export function parseDate(date: string): moment.Moment {
    let result;

    if (moment.utc(new Date(date)).isValid()) {
        result = moment.utc(new Date(date));
    } else {
        const ms = dateMath.parse(date);
        result = moment.utc(ms);
    }

    return result;
}

function determineDivisions(numOfDivisions: number, endingNum: number) {
    const buckets = times(numOfDivisions, () => 1);
    const length = numOfDivisions - 1;
    let remaining = endingNum - numOfDivisions;
    let index = 0;

    while (remaining > 0) {
        buckets[index] += 1;
        index += 1;
        if (index > length) index = 0;
        remaining -= 1;
    }

    return buckets;
}

function compactDivisions(
    _recoveryData: SlicerDateResults[],
    buckets: number[],
    id: number
): DateRanges {
    const recoveryData = _recoveryData.slice();
    const holes: DateConfig[] = [];
    // we condense recoveryDate to the appropriate buckets
    const compactedDivision = buckets.reduce<SlicerDateResults[][]>((list, num) => {
        const pocket: SlicerDateResults[] = [];
        for (let i = 0; i < num; i += 1) {
            const data = recoveryData.shift() as SlicerDateResults;
            pocket.push(data);
        }
        list.push(pocket);
        return list;
    }, []);

    const segment = compactedDivision[id];

    const results: Partial<DateRanges> = {
        start: moment.utc(segment[0].end),
        limit: moment.utc(segment[segment.length - 1].limit)
    };

    segment.forEach((dates, index, arr) => {
        if (arr[index + 1] !== undefined) {
            holes.push({ start: dates.limit, end: arr[index + 1].end });
            // we save existing holes, we do this second to maintain order
            if (dates.holes) holes.push(...dates.holes);
        }
    });

    if (holes.length > 0) {
        results.holes = holes;
    }

    return results as DateRanges;
}

function expandDivisions(
    recoveryData: SlicerDateResults[],
    buckets: number[],
    id: number
): DateRanges {
    const newRanges = buckets.reduce<DateRanges[]>((list, newDivisions, index) => {
        const dates = recoveryData[index];
        const range = divideRange(moment.utc(dates.end), moment.utc(dates.limit), newDivisions);
        list.push(...range);
        return list;
    }, []);

    return newRanges[id];
}

function redistributeDates(
    recoveryData: SlicerDateResults[],
    numOfSlicers: number,
    id: number,
) {
    // we are creating more ranges
    if (numOfSlicers > recoveryData.length) {
        const buckets = determineDivisions(recoveryData.length, numOfSlicers);
        return expandDivisions(recoveryData, buckets, id);
    }
    // we are compacting ranges
    const buckets = determineDivisions(numOfSlicers, recoveryData.length);
    return compactDivisions(recoveryData, buckets, id);
}

export function divideRange(
    startTime: moment.Moment,
    endTime: moment.Moment,
    numOfSlicers: number,
): DateSegments[] {
    const results: DateSegments[] = [];
    // 'x' is Unix Millisecond Timestamp format
    const startNum = Number(moment.utc(startTime).format('x'));
    const limitNum = Number(moment.utc(endTime).format('x'));
    const range = (limitNum - startNum) / numOfSlicers;

    const step = moment.utc(startTime);

    for (let i = 0; i < numOfSlicers; i += 1) {
        const start = moment.utc(step);
        const limit = moment.utc(step.add(range, 'ms'));
        results.push({ start, limit });
    }

    // make sure that end of last segment is always correct
    const endingDate = moment.utc(endTime);
    results[results.length - 1].limit = endingDate;
    return results;
}

// used by stream processing
export function delayedStreamSegment(
    startTime: moment.Moment,
    processingInterval: ParsedInterval,
    latencyInterval: ParsedInterval
): { start: moment.Moment, limit: moment.Moment } {
    const now = moment.utc(startTime);

    const delayedLimit = moment.utc(now).subtract(
        latencyInterval[0],
        latencyInterval[1]
    );

    const delayedStart = moment.utc(delayedLimit).subtract(
        processingInterval[0],
        processingInterval[1]
    );

    return { start: delayedStart, limit: delayedLimit };
}

interface StartingConfig {
    dates: SlicerDateConfig;
    range: DateSegments;
}

function convertToHole(rRecord: SlicerDateResults): DateConfig {
    return { start: moment.utc(rRecord.start), end: moment.utc(rRecord.end) };
}

function holeAffectsRange(dates: DateRanges, hRange: DateConfig): boolean {
    const { start, end } = hRange;
    if (moment.utc(start).isBetween(dates.start, dates.limit)) return true;
    if (moment.utc(end).isBetween(dates.start, dates.limit)) return true;
    if (dates.limit.isBetween(start, end)) return true;

    if (moment.utc(start).isSame(dates.start) || moment.utc(end).isSame(dates.limit)) {
        return true;
    }

    return false;
}

function compareDatesToLimit(dates: SlicerDateConfig) {
    if (dates.end.isSameOrAfter(dates.limit)) dates.end = moment.utc(dates.limit);
    if (dates.start.isSameOrAfter(dates.limit)) dates.start = moment.utc(dates.limit);
    return dates;
}

function compareRangeToRecoveryData(
    newDates: DateRanges,
    recoveryData: SlicerDateResults[],
    interval: ParsedInterval,
    id: number,
    numOfSlicers: number
): SlicerDateConfig {
    const [step, unit] = interval;
    const rData: RDate = recoveryData[id] as RDate;
    const finalDates = Object.assign({}, newDates) as Partial<SlicerDateConfig> & DateRanges;
    const holes = [];

    // expansion of slicers already takes into account the end
    // we need this for exact match and compaction
    if (rData && recoveryData.length >= numOfSlicers) {
        finalDates.start = moment.utc(recoveryData[id].end);
        finalDates.end = moment.utc(finalDates.start).add(step, unit);
        if (rData.holes) holes.push(...rData.holes);
    } else {
        finalDates.end = moment.utc(finalDates.start).add(step, unit);
        const tempHole: DateConfig[] = [];

        recoveryData.forEach((rDates) => {
            tempHole.push(convertToHole(rDates));
            if (rDates.holes) tempHole.push(...rDates.holes);
        });
        // we don't have a specific recoveryData, so we compare holes on all of them
        const holesForDates = tempHole.filter((rRange) => holeAffectsRange(finalDates, rRange));
        holes.push(...holesForDates);
    }

    if (holes.length > 0) {
        const finalHole = holes[holes.length - 1];

        if (finalDates.limit.isSame(finalHole.end)) {
            finalDates.limit = moment.utc(finalHole.start);
            // we encapsulate the hole so we can drop it
            holes.pop();
        } else if (finalDates.limit.isBefore(finalHole.end)) {
            // we keep hole for future boundary increase
            finalDates.limit = moment.utc(finalHole.start);

            if (finalDates.start.isSameOrAfter(finalDates.limit)) {
                finalDates.start = moment.utc(finalDates.limit);
                finalDates.end = moment.utc(finalDates.limit);
            }
        }

        // this would happen if previous end was next to hole
        if (holes[0] && finalDates.start.isSameOrAfter(holes[0].start)) {
            let newStart = moment.utc(holes[0].end);
            // we hole exists beyond limit, we preserve it
            if (newStart.isAfter(finalDates.limit)) {
                newStart = moment.utc(holes[0].start);
            } else {
                // we encapsulate hole so toss it
                holes.shift();
            }

            if (newStart.isSame(finalDates.limit)) {
                newStart = moment.utc(holes[0].start);
            }

            finalDates.start = newStart;
            // we get rid of old hole since we jumped it
        }

        let end = moment.utc(finalDates.start).add(step, unit);
        // we check again because we could have jump the hole already
        if (holes.length > 0) {
            if (end.isSameOrAfter(holes[0].start)) {
                end = moment.utc(holes[0].start);
            }
        }

        finalDates.end = end;
        finalDates.holes = holes;
    }

    return compareDatesToLimit(finalDates as SlicerDateConfig);
}

interface DateRanges {
    start: moment.Moment;
    limit: moment.Moment;
    holes?: DateConfig[];
}

type RDate = SlicerDateResults|undefined;

export function determineStartingPoint(config: StartPointConfig): StartingConfig {
    const {
        dates,
        id,
        numOfSlicers,
        interval,
        recoveryData
    } = config;
    // we need to split up times
    const [step, unit] = interval;
    // we are running in recovery
    if (recoveryData && recoveryData.length > 0) {
        let newDates: DateRanges;

        // our number of slicers have changed
        if (numOfSlicers !== recoveryData.length) {
            newDates = redistributeDates(recoveryData, numOfSlicers, id);
        } else {
            // numOfSlicers are the same so we can distribute normally
            newDates = divideRange(
                dates.start,
                dates.limit,
                numOfSlicers
            )[id];
        }
        const correctDates = compareRangeToRecoveryData(
            newDates, recoveryData, interval, id, numOfSlicers
        );

        return { dates: correctDates, range: dates };
    }

    const dateRange: Partial<SlicerDateConfig>[] = divideRange(
        dates.start,
        dates.limit,
        numOfSlicers
    );

    const newDates = dateRange[id];
    let end = moment.utc(newDates.start).add(step, unit);
    if (end.isSameOrAfter(newDates.limit)) end = moment.utc(newDates.limit);
    newDates.end = end;

    return { dates: newDates as SlicerDateConfig, range: dates };
}

export function buildQuery(opConfig: ReaderConfig, slice: BuildQueryInput): SearchParams {
    const query: SearchParams = {
        index: opConfig.index,
        size: slice.count,
        body: _buildRangeQuery(opConfig, slice),
    };

    if (opConfig.fields) query._source = opConfig.fields;

    return query;
}

function _buildRangeQuery(opConfig: ReaderConfig, slice: BuildQueryInput) {
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
    // TODO: deprecate this logic and remove in the future
    // elasticsearch _id based query
    if (slice.key) {
        body.query.bool.must.push({ wildcard: { _uid: slice.key } });
    }

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

export function validateGeoParameters(opConfig: ReaderConfig): void {
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

export function geoSearch(opConfig: ReaderConfig): AnyObject {
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
