import { cloneDeep, TSError } from '@terascope/job-components';
import moment from 'moment';
import idSlicer from '../../id_reader/id-slicer';
import {
    SlicerArgs,
    SlicerDateResults,
    SlicerDateConfig,
    ParsedInterval,
    DateConfig,
    StartPointConfig,
    DateSegments
} from '../interfaces';
import {
    dateFormat as dFormat,
    dateFormatSeconds,
    dateOptions,
    retryModule,
    determineStartingPoint,
} from '../../__lib';
import { ESIDSlicerArgs } from '../../id_reader/interfaces';
import { getKeyArray } from '../../id_reader/helpers';

interface DetermineSliceResults {
    start: moment.Moment;
    end: moment.Moment;
    count: number;
    key?: string;
}

interface DateParams {
    start: moment.Moment;
    end: moment.Moment;
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

    if (moment(start).add(diff, 'ms').isAfter(limit)) {
        diff = moment(limit).diff(start);
    }

    if (timeResolution === 'ms') {
        return diff;
    }

    const secondDiff = Math.floor(diff / 1000);
    return secondDiff;
}

export default function newSlicer(args: SlicerArgs) {
    const {
        events,
        opConfig,
        executionConfig,
        logger,
        api,
        dates: sliceDates,
        id,
        interval,
        latencyInterval,
        primaryRange,
        windowState
    } = args;

    const timeResolution = dateOptions(opConfig.time_resolution);
    const retryError = retryModule(logger, executionConfig.max_retries);
    const dateFormat = timeResolution === 'ms' ? dFormat : dateFormatSeconds;
    const currentWindow = primaryRange || {} as DateSegments;

    if (executionConfig.lifecycle === 'persistent' && windowState == null) {
        throw new Error('WindowState must be provided if lifecyle is persistent');
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
                const newStart = moment(end).subtract(step, unit);
                // get diff from new start
                const diff = splitTime(newStart, end, limit, timeResolution);
                const newEnd = moment(newStart).add(diff, timeResolution);
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
                    false
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
            const newEnd = moment(start).add(diff, timeResolution);

            // prevent recursive call if difference is one millisecond
            if (diff <= 0) {
                return { start, end, count };
            }

            // recurse to find smaller chunk
            dateParams.end = newEnd;
            events.emit('slicer:slice:recursion');

            if (logger.level() === 10) logger.trace(`slicer: ${slicerId} is recursing ${JSON.stringify(dateParams)}`);

            return determineSlice(dateParams, slicerId, isExpandedSlice);
        }

        // interval is only passed in with once mode, it will expand slices to prevent
        // counts of 0, if the limit is reached it will run once more for the correct count
        // then it should return and not recurse further if there is still no data
        if (!isLimitQuery && count === 0 && dateParams.interval) {
            // increase the slice range to find documents
            let makeLimitQuery = false;

            const newEnd = moment(dateParams.end).add(step, unit);
            if (newEnd.isSameOrAfter(dateParams.limit)) {
                // set to limit
                makeLimitQuery = true;
                dateParams.end = dateParams.limit;
            } else if (holes.length > 0 && newEnd.isSameOrAfter(holes[0].start)) {
                makeLimitQuery = true;
                dateParams.end = moment(holes[0].start);
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
        const idConfig = Object.assign({}, opConfig, { starting_key_depth: 0 });
        const range: SlicerDateResults = Object.assign(
            data,
            {
                start: data.start.format(),
                end: data.end.format(),
                limit
            }
        );

        const idSlicerArs: ESIDSlicerArgs = {
            events,
            opConfig: idConfig,
            executionConfig,
            logger,
            api,
            range,
            keySet: getKeyArray(opConfig)
        };
        const idSlicers = idSlicer(idSlicerArs);

        return getIdData(idSlicers);
    }

    async function getCount(dates: DateParams) {
        const end = dates.end ? dates.end : dates.limit;
        const range: any = {
            start: dates.start.format(dateFormat),
            end: end.format(dateFormat)
        };
        const query = api.buildQuery(opConfig, range);

        return api.count(query);
    }

    async function nextRange() {
        if (executionConfig.lifecycle === 'persistent') {
            const canProcessNextRange = windowState?.checkin(id);
            if (!canProcessNextRange) return null;

            const [step, unit] = interval as ParsedInterval;
            const [lStep, lUnit] = latencyInterval as ParsedInterval;
            const delayedBarrier = moment().subtract(lStep, lUnit);

            const { start, limit } = currentWindow as DateSegments;

            const newStart = moment(start).add(step, unit);
            const newLimit = moment(limit).add(step, unit);

            const config: StartPointConfig = {
                dates: { start: moment(newStart), limit: moment(newLimit) },
                id,
                numOfSlicers: executionConfig.slicers,
                interval
            };

            const { dates } = await determineStartingPoint(config);
            if (dates.limit.isSameOrBefore(delayedBarrier)) {
                // we have succesfuly jumped, move window
                currentWindow.start = newStart;
                currentWindow.limit = newLimit;
                return dates;
            }
            return null;
        }
        return null;
    }

    function dateSlicer(dates: SlicerDateConfig, slicerId: number) {
        const shouldDivideByID = opConfig.subslice_by_key;
        const threshold = opConfig.subslice_key_threshold;
        const holes: DateConfig[] = dates.holes ? dates.holes.slice() : [];
        const [step, unit] = interval as ParsedInterval;

        const dateParams: DateParams = {
            size: opConfig.size,
            interval,
            start: moment(dates.start),
            end: moment(dates.end),
            holes,
            limit: moment(dates.limit)
        };

        logger.debug('all date configurations for date slicer', dateParams);

        return async function sliceDate(msg: any): Promise<SliceResults> {
            if (dateParams.start.isSameOrAfter(dateParams.limit)) {
                // we are done
                // if steaming and there is more work, then continue
                const next = await nextRange();
                // return null to finish or if unable to start new segment
                if (!next) return next;

                const { start, end, limit } = next;
                // TODO: check if we jumped a hole here at start, remove hole
                dateParams.start = moment(start);
                dateParams.end = moment(end);
                dateParams.limit = moment(limit);
            }

            let data: DetermineSliceResults;

            try {
                data = await determineSlice(dateParams, slicerId, false);
            } catch (err) {
                const retryInput = dateParams.start.format(dateFormat);
                return retryError(retryInput, err, sliceDate, msg);
            }

            dateParams.start = moment(data.end);

            if (holes.length > 0 && dateParams.start.isSameOrAfter(holes[0].start)) {
                // we are in a hole, need to shift where it is looking at
                // we mutate on pupose, eject hole that is already passed
                const hole = holes.shift() as DateConfig;
                dateParams.start = moment(hole.end);
                // TODO: check for limit here
            }

            const newEnd = moment(dateParams.start).add(step, unit);

            if (newEnd.isAfter(dateParams.limit)) {
                dateParams.end = moment(data.end).add(dateParams.limit.diff(data.end), 'ms');
            } else if (holes.length > 0 && newEnd.isSameOrAfter(holes[0].start)) {
                dateParams.end = moment(holes[0].start);
            } else {
                dateParams.end = newEnd;
            }

            if (shouldDivideByID && data.count >= threshold) {
                logger.debug('date slicer is recursing by keylist');
                try {
                    const list = await makeKeyList(data, dateParams.limit.format(dateFormat));
                    return list.map((obj) => {
                        obj.limit = dateParams.limit.format(dateFormat);
                        return obj;
                    }) as SlicerDateResults[];
                } catch (err) {
                    return Promise.reject(new TSError(err, { reason: 'error while subslicing by key' }));
                }
            }

            return {
                start: data.start.format(dateFormat),
                end: data.end.format(dateFormat),
                limit: dateParams.limit.format(dateFormat),
                holes: dateParams.holes,
                count: data.count
            };
        };
    }

    return dateSlicer(sliceDates, id);
}


