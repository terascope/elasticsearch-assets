import {
    Logger, parseError, times
} from '@terascope/job-components';
import moment from 'moment';
import fs from 'fs';
// @ts-ignore
import dateMath from 'datemath-parser';

import {
    StartPointConfig,
    SlicerDateConfig,
    DateSegments,
    SlicerDateResults,
    ParsedInterval,
    DateConfig
} from './elasticsearch_reader/interfaces';

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
    esDates?: any
) {
    // one or more digits, followed by one or more letters, case-insensitive
    const regex = /(\d+)(\D+)/i;
    const intervalMatch = regex.exec(interval);

    if (intervalMatch === null) {
        throw new Error('elasticsearch_reader interval and/or delay are incorrectly formatted. Needs to follow [number][letter\'s] format, e.g. "12s"');
    }

    // dont need first parameter, its the full string
    intervalMatch.shift();

    intervalMatch[1] = dateOptions(intervalMatch[1]);
    return compareInterval(intervalMatch, esDates, timeResolution);
}

function compareInterval(interval: any, esDates: any, timeResolution: string) {
    if (esDates) {
        const datesDiff = esDates.limit.diff(esDates.start);
        const intervalDiff = moment.duration(Number(interval[0]), interval[1]).as('milliseconds');

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

type RetryFn = (msg: string) => any

export function retryModule(logger: Logger, numOfRetries: number) {
    const retry = {};
    return (_key: string | object, err: Error, fn: RetryFn, msg: string) => {
        const errMessage = parseError(err);
        logger.error('error while getting next slice', errMessage);
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

export function existsSync(filename: string) {
    try {
        fs.accessSync(filename);
        return true;
    } catch (ex) {
        return false;
    }
}

export function getMilliseconds(interval: any[]) {
    const conversions = {
        d: 86400000,
        h: 3600000,
        m: 60000,
        s: 1000,
        ms: 1
    };

    return interval[0] * conversions[interval[1]];
}

export function parseDate(date: string) {
    let result;

    if (moment(new Date(date)).isValid()) {
        result = moment(new Date(date));
    } else {
        const ms = dateMath.parse(date);
        result = moment(ms);
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
    recoveryData: SlicerDateResults[],
    buckets: number[],
    interval: ParsedInterval,
    id: number
): SlicerDateConfig {
    const [intervalNum, intervalUnit] = interval;
    const holes: DateConfig[] = [];
    // we condense recoveryDate to the appopriate buckets
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

    const results: Partial<SlicerDateConfig> = {
        start: moment(segment[0].end),
        limit: moment(segment[segment.length - 1].limit)
    };

    segment.forEach((dates, index, arr) => {
        if (arr[index + 1] !== undefined) {
            // we save existing holes
            if (dates.holes) holes.push(...dates.holes);
            holes.push({ start: dates.limit, end: arr[index + 1].end });
        }
    });

    let end = moment(results.start).add(intervalNum, intervalUnit);
    if (end.isSameOrAfter(results.limit)) end = moment(results.limit);

    if (holes.length > 0) {
        results.holes = holes;
        // we check to see if end is in a hole
        holes.forEach((hole) => {
            if (moment(end).isSameOrAfter(hole.start)) {
                // we stop at the start of a hole
                end = moment(hole.start);
            }
        });
    }

    results.end = end;
    return results as SlicerDateConfig;
}

function expandDivisions(
    recoveryData: SlicerDateResults[],
    buckets: number[],
    interval: ParsedInterval,
    id: number
) {
    const [intervalNum, intervalUnit] = interval;

    const newRanges = buckets.reduce<SlicerDateConfig[]>((list, newDivisions, index) => {
        const dates = recoveryData[index];
        const range = divideRange(moment(dates.end), moment(dates.limit), newDivisions)
            .map((val: Partial<SlicerDateConfig>) => {
                let end = moment(val.start).add(intervalNum, intervalUnit);
                if (end.isSameOrAfter(val.limit)) end = moment(val.limit);
                // TODO: check for holes here
                val.end = end;
                return val as SlicerDateConfig;
            });

        list.push(...range);
        return list;
    }, []);

    return newRanges[id];
}

function redistributeDates(
    recoveryData: SlicerDateResults[],
    numOfSlicers: number,
    id: number,
    interval: ParsedInterval
): SlicerDateConfig {
    // we are creating more ranges
    if (numOfSlicers > recoveryData.length) {
        const buckets = determineDivisions(recoveryData.length, numOfSlicers);
        return expandDivisions(recoveryData, buckets, interval, id);
    }
    // we are compacting ranges
    const buckets = determineDivisions(numOfSlicers, recoveryData.length);
    return compactDivisions(recoveryData, buckets, interval, id);
}

export function divideRange(
    startTime: moment.Moment,
    endTime: moment.Moment,
    numOfSlicers: number,
) {
    const results: DateSegments[] = [];
    // 'x' is Unix Millisecond Timestamp format
    const startNum = Number(moment(startTime).format('x'));
    const limitNum = Number(moment(endTime).format('x'));
    const range = (limitNum - startNum) / numOfSlicers;

    const step = moment(startTime);

    for (let i = 0; i < numOfSlicers; i += 1) {
        // TODO: this mutates step
        const start = moment(step);
        const limit = moment(step.add(range, 'ms'));
        results.push({ start, limit });
    }

    // make sure that end of last segment is always correct
    const endingDate = moment(endTime);
    results[results.length - 1].limit = endingDate;
    return results;
}

export function determineStartingPoint(config: StartPointConfig): SlicerDateConfig {
    const {
        dates,
        id,
        numOfSlicers,
        interval,
        recoveryData
    } = config;
    // we need to split up times
    const [intervalNum, intervalUnit] = interval;
    // we are running in recovery
    if (recoveryData && recoveryData.length > 0) {
        // TODO: review SlicerDateConfig
        let newDates: Partial<SlicerDateConfig> = {};

        // our number of slicers have changed
        if (numOfSlicers !== recoveryData.length) {
            newDates = redistributeDates(recoveryData, numOfSlicers, id, interval);
            return newDates as SlicerDateConfig;
        }
        // numOfSlicers are the same so we can distribute normally
        // TODO: this is probably faulty behavior
        newDates = divideRange(
            dates.start,
            dates.limit,
            numOfSlicers
        )[id];
        // @ts-ignore FIXME:
        const recoveryEnd = moment(recoveryData[id].end);
        newDates.start = recoveryEnd;

        let end = moment(newDates.start).add(intervalNum, intervalUnit);
        if (end.isSameOrAfter(newDates.limit)) end = moment(newDates.limit);
        newDates.end = end;

        return newDates as SlicerDateConfig;
    }

    const newDates: Partial<SlicerDateConfig> = divideRange(
        dates.start,
        dates.limit,
        numOfSlicers
    )[id];

    let end = moment(newDates.start).add(intervalNum, intervalUnit);
    if (end.isSameOrAfter(newDates.limit)) end = moment(newDates.limit);
    newDates.end = end;

    return newDates as SlicerDateConfig;
}
