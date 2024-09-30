import {
    cloneDeep, isNumber, TSError,
    isString
} from '@terascope/utils';
import moment from 'moment';
import { inspect } from 'node:util';
import { idSlicer } from './idSlicer.js';
import {
    SlicerArgs,
    SlicerDateConfig,
    ParsedInterval,
    DateConfig,
    StartPointConfig,
    DateSegments,
    DetermineSliceResults,
    IDSlicerArgs,
    IDType,
    DateSlicerResults,
    ReaderSlice
} from '../interfaces.js';
import {
    dateFormat as dFormat,
    dateFormatSeconds,
    dateOptions,
    determineDateSlicerRange,
} from './date-helpers.js';
import { getKeyArray } from './id-helpers.js';

interface DateParams {
    start: moment.Moment;
    end: moment.Moment;
    prevEnd?: moment.Moment;
    limit: moment.Moment;
    holes: DateConfig[];
    interval: ParsedInterval;
    size: number;
}

function splitTime(
    start: moment.Moment,
    end: moment.Moment,
    limit: moment.Moment,
    timeResolution: string
) {
    let diff = Math.floor(end.diff(start) / 2);

    if (moment.utc(start).add(diff, 'ms')
        .isAfter(limit)) {
        diff = moment.utc(limit).diff(start);
    }

    if (timeResolution === 'ms') {
        return diff;
    }

    const secondDiff = Math.floor(diff / 1000);
    return secondDiff;
}

export function dateSlicer(args: SlicerArgs): () => Promise<DateSlicerResults> {
    const {
        events,
        timeResolution: timeResolutionArg,
        size: querySize,
        numOfSlicers,
        lifecycle,
        logger,
        dates: sliceDates,
        id,
        latencyInterval,
        primaryRange,
        windowState,
        countFn,
        subsliceByKey,
        subsliceKeyThreshold,
        idFieldName = null,
        startingKeyDepth = 0,
        keyType = IDType.base64url
    } = args;

    if (!args.interval) {
        throw new Error('Missing parameter interval');
    }
    const interval = args.interval!;

    if (subsliceByKey) {
        if (!isNumber(subsliceKeyThreshold)) {
            throw new Error('Invalid parameter subsliceKeyThreshold, it must be set to a number if subsliceByKey is set to true');
        }

        if (!isString(idFieldName)) {
            throw new Error('Invalid parameter idFieldName, it must be set to a string if subsliceByKey is set to true');
        }
    }

    const isPersistent = lifecycle === 'persistent';
    const timeResolution = dateOptions(timeResolutionArg);
    const dateFormat = timeResolution === 'ms' ? dFormat : dateFormatSeconds;
    const currentWindow = primaryRange || {} as DateSegments;

    if (isPersistent && windowState == null) {
        throw new Error('WindowState must be provided if lifecycle is persistent');
    }

    async function determineSlice(
        dateParams: DateParams, slicerId: number, isExpandedSlice?: boolean, isLimitQuery?: boolean
    ): Promise<DetermineSliceResults> {
        const {
            start, end, limit, size, holes, interval: [step, unit]
        } = dateParams;

        if (!end.isValid()) {
            throw new Error(`Received an invalid end date. ${inspect({ dateParams })}`);
        }

        let count: number;
        try {
            count = await getCount(dateParams);
        } catch (err) {
            throw new TSError(err, {
                reason: `Unable to count slice ${inspect(dateParams)}`
            });
        }

        if (count > size) {
            // if size is to big after increasing slice, use alternative division behavior
            if (isExpandedSlice) {
                // recurse down to the appropriate size
                const newStart = moment.utc(dateParams.prevEnd);
                // get diff from new start
                const diff = splitTime(newStart, end, limit, timeResolution);
                const newEnd = moment.utc(newStart).add(diff, timeResolution);

                if (!newEnd.isValid()) {
                    throw new Error(`Calculated an invalid end date. ${inspect({ dateParams })}`);
                }

                const cloneDates: DateParams = {
                    interval: dateParams.interval,
                    limit,
                    size,
                    holes,
                    start: newStart,
                    end: newEnd,
                };

                const data: DetermineSliceResults = await determineSlice(
                    cloneDates,
                    slicerId,
                    false,
                    isLimitQuery
                );
                // return the zero range start with the correct end
                return {
                    start,
                    end: data.end,
                    count: data.count
                };
            }

            // find difference in milliseconds and divide in half
            const diff = splitTime(start, end, limit, timeResolution);
            const newEnd = moment.utc(start).add(diff, timeResolution);
            // prevent recursive call if difference is one millisecond
            if (diff <= 0) {
                return { start, end, count };
            }

            if (!newEnd.isValid()) {
                throw new Error(`Calculated an invalid end date. ${inspect({ dateParams })}`);
            }
            // recurse to find smaller chunk
            dateParams.end = newEnd;
            events.emit('slicer:slice:recursion');

            if (logger.level() === 10) logger.trace(`slicer: ${slicerId} is recursing ${JSON.stringify(dateParams)}`);

            return determineSlice(dateParams, slicerId, isExpandedSlice, isLimitQuery);
        }

        // with once mode, it will expand slices to prevent
        // counts of 0, if the limit is reached it will run once more for the correct count
        // then it should return and not recurse further if there is still no data
        if (!isPersistent && !isLimitQuery && count === 0) {
            // increase the slice range to find documents
            let makeLimitQuery = false;
            // we make a mark of the last end spot before expansion
            dateParams.prevEnd = moment.utc(end);

            let newEnd = moment.utc(dateParams.end).add(step, unit);
            if (newEnd.isSameOrAfter(dateParams.limit)) {
                // set to limit
                makeLimitQuery = true;
                newEnd = moment.utc(dateParams.limit);
            } else if (holes.length > 0 && newEnd.isSameOrAfter(holes[0].start)) {
                makeLimitQuery = true;
                newEnd = moment.utc(holes[0].start);
            }

            if (!newEnd.isValid()) {
                throw new Error(`Calculated an invalid end date. ${inspect({ dateParams, holes })}`);
            }

            dateParams.end = newEnd;

            events.emit('slicer:slice:range_expansion');
            return determineSlice(dateParams, slicerId, true, makeLimitQuery);
        }

        return {
            start: dateParams.start,
            end: dateParams.end,
            count
        };
    }

    async function getIdData(slicerFn: any): Promise<ReaderSlice[]> {
        const list: ReaderSlice[] = [];
        return new Promise(((resolve, reject) => {
            const slicer = slicerFn;
            function iterate() {
                Promise.resolve(slicer())
                    .then((data) => {
                        if (data) {
                            list.push(cloneDeep(data));
                            return iterate();
                        }
                        return resolve(list);
                    })
                    .catch((err) => {
                        // retries happen at the idSlicer level
                        reject(new TSError(err, {
                            reason: 'error trying to subslice by key'
                        }));
                    });
            }

            iterate();
        }));
    }

    async function makeKeyList(data: DetermineSliceResults, limit: string) {
        const dates = {
            start: moment(data.start.format(dateFormat)).toISOString(),
            end: moment(data.end.format(dateFormat)).toISOString(),
            limit: moment(moment(limit).format(dateFormat)).toISOString(),
        };

        const range: ReaderSlice = Object.assign(
            data,
            dates
        );

        const keyArray = getKeyArray(keyType);

        const idSlicerArs: IDSlicerArgs = {
            events,
            logger,
            range,
            keySet: keyArray,
            baseKeyArray: keyArray,
            countFn,
            size: querySize,
            startingKeyDepth
        };

        const idSlicers = idSlicer(idSlicerArs);
        return getIdData(idSlicers);
    }

    async function getCount(dates: DateParams): Promise<number> {
        const end = dates.end ? dates.end : dates.limit;
        const query = {
            start: dates.start.format(dateFormat),
            end: end.format(dateFormat)
        };

        return countFn(query);
    }

    async function nextRange() {
        if (isPersistent) {
            const canProcessNextRange = windowState?.checkin(id);

            if (!canProcessNextRange) return null;

            const [step, unit] = interval;
            const [lStep, lUnit] = latencyInterval as ParsedInterval;
            const delayedBarrier = moment.utc().subtract(lStep, lUnit);

            const { start, limit } = currentWindow as DateSegments;

            const newStart = moment.utc(start).add(step, unit);
            const newLimit = moment.utc(limit).add(step, unit);

            const config: StartPointConfig = {
                dates: { start: moment.utc(newStart), limit: moment.utc(newLimit) },
                numOfSlicers,
                getInterval() {
                    // we don't actually need the count here
                    return { interval, count: null };
                }
            };

            const result = await determineDateSlicerRange(config, id);
            if (result == null) {
                throw new Error('Got back null when it should have returned a date since the interval is fixed');
            }

            if (result.dates.limit.isSameOrBefore(delayedBarrier)) {
                // we have successfully jumped, move window
                currentWindow.start = newStart;
                currentWindow.limit = newLimit;
                return result.dates;
            }
            return null;
        }
        return null;
    }

    function adjustDates(dateParams: DateParams, holes: DateConfig[]) {
        const [step, unit] = interval;

        if (holes.length > 0 && dateParams.start.isSameOrAfter(holes[0].start)) {
            // we are in a hole, need to shift where it is looking at
            // we mutate on purpose, eject hole that is already passed
            const hole = holes.shift() as DateConfig;
            let newStart = moment.utc(hole.end);

            if (newStart.isAfter(dateParams.limit)) {
                newStart = moment.utc(dateParams.limit);
            }

            dateParams.start = newStart;
        }

        let newEnd = moment.utc(dateParams.start).add(step, unit);

        if (newEnd.isSameOrAfter(dateParams.limit)) {
            newEnd = moment.utc(dateParams.limit);
        } else if (holes.length > 0 && newEnd.isSameOrAfter(holes[0].start)) {
            newEnd = moment.utc(holes[0].start);
        }
        if (!newEnd.isValid()) {
            throw new Error(`Calculated an invalid end date. ${inspect({ dateParams, holes })}`);
        }
        dateParams.end = newEnd;
    }

    function makeDateSlicer(
        dates: SlicerDateConfig,
        slicerId: number
    ): () => Promise<DateSlicerResults> {
        const threshold = subsliceKeyThreshold as number;
        const holes: DateConfig[] = dates.holes ? dates.holes.slice() : [];

        const dateParams: DateParams = {
            size: querySize,
            interval,
            start: moment.utc(dates.start),
            end: moment.utc(dates.end),
            holes,
            limit: moment.utc(dates.limit)
        };

        logger.debug('all date configurations for date slicer', dateParams);

        return async function sliceDate(): Promise<DateSlicerResults> {
            if (dateParams.start.isSameOrAfter(dateParams.limit)) {
                // we are done
                // if steaming and there is more work, then continue
                const next = await nextRange();
                // return null to finish or if unable to start new segment
                if (!next) return next;

                const { start, end, limit } = next;
                if (!end.isValid()) {
                    throw new Error(`Calculated an invalid end date. ${inspect({ next })}`);
                }

                // TODO: check if we jumped a hole here at start, remove hole
                dateParams.start = moment.utc(start);
                dateParams.end = moment.utc(end);
                dateParams.limit = moment.utc(limit);
                adjustDates(dateParams, holes);
            }

            const data = await determineSlice(dateParams, slicerId, false);

            dateParams.start = moment.utc(data.end);

            adjustDates(dateParams, holes);

            if (subsliceByKey && data.count >= threshold) {
                logger.debug('date slicer is recursing by key list');
                try {
                    const list = await makeKeyList(data, dateParams.limit.format(dateFormat));
                    return list.map((obj) => {
                        obj.limit = moment(dateParams.limit.format(dateFormat)).toISOString();
                        return obj;
                    });
                } catch (err) {
                    throw new TSError(err, {
                        reason: 'error while sub-slicing by key'
                    });
                }
            }

            return {
                start: moment(data.start.format(dateFormat)).toISOString(),
                end: moment(data.end.format(dateFormat)).toISOString(),
                limit: moment(dateParams.limit.format(dateFormat)).toISOString(),
                holes: dateParams.holes,
                count: data.count
            };
        };
    }

    if (!sliceDates) {
        throw new Error('Missing parameter sliceDates');
    }
    return makeDateSlicer(sliceDates, id);
}
