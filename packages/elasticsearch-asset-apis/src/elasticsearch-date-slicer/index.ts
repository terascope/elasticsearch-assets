import type { SlicerFn } from '@terascope/job-components';
import { cloneDeep, TSError } from '@terascope/utils';
import moment from 'moment';
import idSlicer from '../elasticsearch-id-slicer';
import {
    SlicerArgs,
    SlicerDateResults,
    SlicerDateConfig,
    ParsedInterval,
    DateConfig,
    StartPointConfig,
    DateSegments,
    DetermineSliceResults,
    IDSlicerArgs
} from '../interfaces';
import {
    dateFormat as dFormat,
    dateFormatSeconds,
    dateOptions,
    determineStartingPoint
} from './helpers';
import { getKeyArray } from '../elasticsearch-id-slicer/helpers';

interface DateParams {
    start: moment.Moment;
    end: moment.Moment;
    prevEnd?: moment.Moment;
    limit: moment.Moment;
    holes: DateConfig[];
    interval: ParsedInterval;
    size: number;
}

type SliceResults = SlicerDateResults | SlicerDateResults[] | null;

function splitTime(
    start: moment.Moment,
    end: moment.Moment,
    limit: moment.Moment,
    timeResolution: string
) {
    let diff = Math.floor(end.diff(start) / 2);

    if (moment.utc(start).add(diff, 'ms').isAfter(limit)) {
        diff = moment.utc(limit).diff(start);
    }

    if (timeResolution === 'ms') {
        return diff;
    }

    const secondDiff = Math.floor(diff / 1000);
    return secondDiff;
}

export default function newSlicer(args: SlicerArgs): SlicerFn {
    const {
        events,
        opConfig,
        numOfSlicers,
        lifecycle,
        logger,
        dates: sliceDates,
        id,
        interval,
        latencyInterval,
        primaryRange,
        windowState,
        countFn,
        version
    } = args;
    const isPersistent = lifecycle === 'persistent';
    const timeResolution = dateOptions(opConfig.time_resolution);
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

        let count: number;
        try {
            count = await getCount(dateParams);
        } catch (err) {
            const error = new TSError(err, { reason: `Unable to count slice ${JSON.stringify(dateParams)}` });
            return Promise.reject(error);
        }

        if (count > size) {
            // if size is to big after increasing slice, use alternative division behavior
            if (isExpandedSlice) {
                // recurse down to the appropriate size
                const newStart = moment.utc(dateParams.prevEnd);
                // get diff from new start
                const diff = splitTime(newStart, end, limit, timeResolution);
                const newEnd = moment.utc(newStart).add(diff, timeResolution);

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

            const newEnd = moment.utc(dateParams.end).add(step, unit);
            if (newEnd.isSameOrAfter(dateParams.limit)) {
                // set to limit
                makeLimitQuery = true;
                dateParams.end = moment.utc(dateParams.limit);
            } else if (holes.length > 0 && newEnd.isSameOrAfter(holes[0].start)) {
                makeLimitQuery = true;
                dateParams.end = moment.utc(holes[0].start);
            } else {
                dateParams.end = newEnd;
            }

            events.emit('slicer:slice:range_expansion');
            return determineSlice(dateParams, slicerId, true, makeLimitQuery);
        }

        return {
            start: dateParams.start,
            end: dateParams.end,
            count
        };
    }

    async function getIdData(slicerFn: any): Promise<Partial<SlicerDateResults>[]> {
        const list: Partial<SlicerDateResults>[] = [];
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

        const range: SlicerDateResults = Object.assign(
            data,
            dates
        );

        const {
            type,
            id_field_name,
            size,
            starting_key_depth
        } = opConfig;

        const keyArray = getKeyArray(opConfig.key_type);

        const idSlicerArs: IDSlicerArgs = {
            events,
            logger,
            range,
            keySet: keyArray,
            baseKeyArray: keyArray,
            countFn,
            version,
            type,
            idFieldName: id_field_name,
            size,
            startingKeyDepth: starting_key_depth
        };

        const idSlicers = idSlicer(idSlicerArs);

        return getIdData(idSlicers);
    }

    async function getCount(dates: DateParams) {
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

            const [step, unit] = interval as ParsedInterval;
            const [lStep, lUnit] = latencyInterval as ParsedInterval;
            const delayedBarrier = moment.utc().subtract(lStep, lUnit);

            const { start, limit } = currentWindow as DateSegments;

            const newStart = moment.utc(start).add(step, unit);
            const newLimit = moment.utc(limit).add(step, unit);

            const config: StartPointConfig = {
                dates: { start: moment.utc(newStart), limit: moment.utc(newLimit) },
                id,
                numOfSlicers,
                interval
            };

            const { dates } = await determineStartingPoint(config);

            if (dates.limit.isSameOrBefore(delayedBarrier)) {
                // we have successfully jumped, move window
                currentWindow.start = newStart;
                currentWindow.limit = newLimit;
                return dates;
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

        const newEnd = moment.utc(dateParams.start).add(step, unit);

        if (newEnd.isSameOrAfter(dateParams.limit)) {
            dateParams.end = moment.utc(dateParams.limit);
        } else if (holes.length > 0 && newEnd.isSameOrAfter(holes[0].start)) {
            dateParams.end = moment.utc(holes[0].start);
        } else {
            dateParams.end = newEnd;
        }
    }

    function dateSlicer(dates: SlicerDateConfig, slicerId: number): SlicerFn {
        const shouldDivideByID = opConfig.subslice_by_key;
        const threshold = opConfig.subslice_key_threshold;
        const holes: DateConfig[] = dates.holes ? dates.holes.slice() : [];

        const dateParams: DateParams = {
            size: opConfig.size,
            interval,
            start: moment.utc(dates.start),
            end: moment.utc(dates.end),
            holes,
            limit: moment.utc(dates.limit)
        };

        logger.debug('all date configurations for date slicer', dateParams);

        return async function sliceDate(): Promise<SliceResults> {
            if (dateParams.start.isSameOrAfter(dateParams.limit)) {
                // we are done
                // if steaming and there is more work, then continue
                const next = await nextRange();
                // return null to finish or if unable to start new segment
                if (!next) return next;

                const { start, end, limit } = next;
                // TODO: check if we jumped a hole here at start, remove hole
                dateParams.start = moment.utc(start);
                dateParams.end = moment.utc(end);
                dateParams.limit = moment.utc(limit);
                adjustDates(dateParams, holes);
            }

            const data = await determineSlice(dateParams, slicerId, false);

            dateParams.start = moment.utc(data.end);

            adjustDates(dateParams, holes);

            if (shouldDivideByID && data.count >= threshold) {
                logger.debug('date slicer is recursing by key list');
                try {
                    const list = await makeKeyList(data, dateParams.limit.format(dateFormat));
                    return list.map((obj) => {
                        obj.limit = moment(dateParams.limit.format(dateFormat)).toISOString();
                        return obj;
                    }) as SlicerDateResults[];
                } catch (err) {
                    return Promise.reject(new TSError(err, { reason: 'error while sub-slicing by key' }));
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

    return dateSlicer(sliceDates, id);
}
