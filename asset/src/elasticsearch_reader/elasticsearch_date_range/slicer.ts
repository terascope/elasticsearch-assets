import { cloneDeep, TSError } from '@terascope/job-components';
import moment from 'moment';
import idSlicer from '../../id_reader/id-slicer';
import {
    SlicerArgs,
    SlicerDateResults,
    SlicerDateConfig,
    ParsedInterval,
    DateConfig
} from '../interfaces';
import * as helpers from '../../helpers';
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

type SlicerFnResults = SlicerDateResults|SlicerDateResults[]|null;

export default function newSlicer(args: SlicerArgs) {
    const {
        context,
        opConfig,
        executionConfig,
        logger,
        api,
        dates: sliceDates,
        id,
        interval,
        delayTime
    } = args;

    const events = context.apis.foundation.getSystemEvents();
    const timeResolution = helpers.dateOptions(opConfig.time_resolution);
    const retryError = helpers.retryModule(logger, executionConfig.max_retries);
    const dateFormat = timeResolution === 'ms' ? helpers.dateFormat : helpers.dateFormatSeconds;
    // This could be different since we have another op that uses this module

    async function determineSlice(
        dateParams: DateParams, slicerId: number, isExpandedSlice?: boolean, isLimitQuery?: boolean
    ): Promise<DetermineSliceResults> {
        const {
            start, end, limit, size, holes, interval: [intervalNum, intervalUnit]
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
                const newStart = moment(end).subtract(intervalNum, intervalUnit);
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

            const newEnd = moment(dateParams.end).add(intervalNum, intervalUnit);
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
            context,
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

    function boundedSlicer(dates: SlicerDateConfig, slicerId: number) {
        const shouldDivideByID = opConfig.subslice_by_key;
        const threshold = opConfig.subslice_key_threshold;
        const [intervalNum, intervalUnit] = opConfig.interval;
        const holes: DateConfig[] = dates.holes ? dates.holes.slice() : [];

        const limit = moment(dates.limit);
        const start = moment(dates.start);
        const end = moment(dates.end);

        const dateParams: DateParams = {
            size: opConfig.size,
            interval: opConfig.interval,
            start,
            end,
            holes,
            limit
        };

        logger.debug('all date configurations for date slicer', dateParams);

        return async function sliceDate(msg: any): Promise<SlicerFnResults> {
            let data: DetermineSliceResults;

            if (dateParams.start.isSameOrAfter(dateParams.limit)) return null;

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
            }

            const newEnd = moment(dateParams.start).add(intervalNum, intervalUnit);

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
                    const list = await makeKeyList(data, limit.format(dateFormat));
                    return list.map((obj) => {
                        obj.limit = limit.format(dateFormat);
                        return obj;
                    }) as SlicerDateResults[];
                } catch (err) {
                    return Promise.reject(new TSError(err, { reason: 'error while subslicing by key' }));
                }
            }
            // TODO: do we need dataParams.holes here? where should mutate
            return {
                start: data.start.format(dateFormat),
                end: data.end.format(dateFormat),
                limit: limit.format(dateFormat),
                holes: dateParams.holes,
                count: data.count
            };
        };
    }

    function streamSlicer(slicerDates: SlicerDateConfig, slicerId: number) {
        const shouldDivideByID = opConfig.subslice_by_key;
        const threshold = opConfig.subslice_key_threshold;
        const holes: DateConfig[] = slicerDates.holes ? slicerDates.holes.slice() : [];
        const [step, unit] = interval as ParsedInterval;

        const start = moment(slicerDates.start);
        const end = moment(slicerDates.limit);

        let limit = moment(slicerDates.limit);

        const dateParams: DateParams = {
            size: opConfig.size,
            interval: interval as ParsedInterval,
            start,
            end,
            holes,
            limit
        };

        // set a timer to add the next set it should process
        const injector = setInterval(() => {
            // keep a list of next batches in cases current batch is still running
            const newLimit = moment(limit).add(step, unit);
            dateParams.limit = newLimit;
            dateParams.end = newLimit;
            limit = newLimit;
        }, delayTime as number);

        events.on('worker:shutdown', () => clearInterval(injector));

        return async function sliceDate(msg: any): Promise<SlicerFnResults> {
            if (dateParams.start.isSameOrAfter(limit)) return null;
            let data: DetermineSliceResults;

            try {
                data = await determineSlice(dateParams, slicerId, false);
            } catch (err) {
                const retryInput = dateParams.start.format(dateFormat);
                return retryError(retryInput, err, sliceDate, msg);
            }

            dateParams.start = data.end;

            if (moment(data.end).add(step, unit).isAfter(limit)) {
                dateParams.end = moment(data.end).add(dateParams.limit.diff(data.end), 'ms');
            } else {
                dateParams.end = moment(data.end).add(step, unit);
            }

            if (shouldDivideByID && data.count >= threshold) {
                logger.debug('date slicer is recursing by keylist');
                try {
                    const list = await makeKeyList(data, limit.format(dateFormat));
                    return list.map((obj) => {
                        obj.limit = limit.format(dateFormat);
                        return obj;
                    }) as SlicerDateResults[];
                } catch (err) {
                    return Promise.reject(new TSError(err, { reason: 'error while subslicing by key' }));
                }
            }

            return {
                start: data.start.format(dateFormat),
                end: data.end.format(dateFormat),
                limit: limit.format(dateFormat),
                count: data.count
            };
        };
    }

    if (executionConfig.lifecycle === 'persistent') {
        return streamSlicer(sliceDates, id);
    }

    return boundedSlicer(sliceDates, id);
}
