
import {
    cloneDeep, toString, parseError, TSError
} from '@terascope/job-components';
import moment from 'moment';
import idSlicer from '../../id_reader/id-slicer';
import {
    SlicerArgs, SlicerDateResults, DateConfig, ParsedInterval
} from '../interfaces';
import * as helpers from '../../helpers';
import { ESIDSlicerArgs } from '../../id_reader/interfaces';
import { getKeyArray } from '../../id_reader/helpers';

interface SliceResults {
    start: moment.Moment;
    end: moment.Moment;
    count: number;
    key?: string;
}

interface DateParams {
    start: moment.Moment;
    end: moment.Moment;
    limit: moment.Moment;
    interval: ParsedInterval;
    size: number;
}

export default function newSlicer(args: SlicerArgs) {
    const {
        context,
        opConfig,
        executionConfig,
        retryData,
        logger,
        api,
        dates: sliceDates,
        id
    } = args;

    const events = context.apis.foundation.getSystemEvents();
    const timeResolution = helpers.dateOptions(opConfig.time_resolution);
    const retryError = helpers.retryModule(logger, executionConfig.max_retries);
    const dateFormat = timeResolution === 'ms' ? helpers.dateFormat : helpers.dateFormatSeconds;
    // This could be different since we have another op that uses this module

    function splitTime(start: moment.Moment, end: moment.Moment, limit: moment.Moment) {
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

    async function determineSlice(
        dateParams: DateParams, slicerId: number, isExpandedSlice?: boolean, isLimitQuery?: boolean
    ): Promise<SliceResults> {
        const intervalNum = dateParams.interval[0];
        const intervalUnit = dateParams.interval[1];

        let count: number;
        try {
            count = await getCount(dateParams);
        } catch (err) {
            const errMessage = parseError(err);
            logger.error('error with determine slice:', errMessage);
            const error = new Error(`Failure to determine slice: ${toString(err)}`);
            return Promise.reject(error);
        }

        if (count > dateParams.size) {
            // if size is to big after increasing slice, use alternative division behavior
            if (isExpandedSlice) {
            // recurse down to the appropriate size
                const newStart = moment(dateParams.end).subtract(intervalNum, intervalUnit);
                // get diff from new start
                const diff = splitTime(newStart, dateParams.end, dateParams.limit);
                const newEnd = moment(newStart).add(diff, timeResolution);
                const cloneDates: DateParams = {
                    interval: dateParams.interval,
                    limit: dateParams.limit,
                    size: dateParams.size,
                    start: newStart,
                    end: newEnd,
                };

                const data: SliceResults = await determineSlice(cloneDates, slicerId, false);
                // return the zero range start with the correct end
                return {
                    start: dateParams.start,
                    end: data.end,
                    count: data.count
                };
            }

            // find difference in milliseconds and divide in half
            const diff = splitTime(dateParams.start, dateParams.end, dateParams.limit);
            const newEnd = moment(dateParams.start).add(diff, timeResolution);

            // prevent recursive call if difference is one millisecond
            if (diff <= 0) {
                return { start: dateParams.start, end: dateParams.end, count };
            }

            // recurse to find smaller chunk
            dateParams.end = newEnd;
            events.emit('slicer:slice:recursion');
            logger.trace(`slicer: ${slicerId} is recursing ${JSON.stringify(dateParams)}`);

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
            } else {
                dateParams.end = newEnd;
            }
            events.emit('slicer:slice:range_expansion');
            return determineSlice(dateParams, slicerId, true, makeLimitQuery);
        }

        return { start: dateParams.start, end: dateParams.end, count };
    }

    async function getIdData(slicerFn: any) {
        const list: any[] = [];
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
                        const errMessage = parseError(err);
                        logger.error('error trying to subslice by key on getIdData:', errMessage);
                        reject(new TSError(err, {
                            reason: 'error trying to subslice by key'
                        }));
                    });
            }

            iterate();
        }));
    }

    function makeKeyList(data: SliceResults) {
        const idConfig = Object.assign({}, opConfig, { starting_key_depth: 0 });
        const range: SlicerDateResults = Object.assign(
            data,
            { start: data.start.format(), end: data.end.format() }
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

    function nextChunk(dates: DateConfig, slicerId: number, retryDataObj: any) {
        const shouldDivideByID = opConfig.subslice_by_key;
        const threshold = opConfig.subslice_key_threshold;
        const [intervalNum, intervalUnit] = opConfig.interval;
        const limit = moment(dates.end);
        let start = moment(dates.start);

        if (retryDataObj && retryDataObj.lastSlice && retryDataObj.lastSlice.end) {
            start = moment(retryDataObj.lastSlice.end);
        }

        let end = moment(start.format(dateFormat)).add(intervalNum, intervalUnit);
        if (end.isSameOrAfter(limit)) end = limit;

        const dateParams: DateParams = {
            size: opConfig.size,
            interval: opConfig.interval,
            start,
            end,
            limit
        };

        logger.debug('all date configurations for date slicer', dateParams);

        return async function sliceDate(msg: any) {
            if (dateParams.start.isSameOrAfter(dateParams.limit)) {
                return null;
            }
            let data: SliceResults;
            try {
                data = await determineSlice(dateParams, slicerId, false);
            } catch (err) {
                const retryInput = dateParams.start.format(dateFormat);
                return retryError(retryInput, err, sliceDate, msg);
            }

            dateParams.start = data.end;

            if (moment(data.end).add(intervalNum, intervalUnit) > dateParams.limit) {
                dateParams.end = moment(data.end).add(dateParams.limit.diff(data.end));
            } else {
                dateParams.end = moment(data.end).add(intervalNum, intervalUnit);
            }

            if (shouldDivideByID && data.count >= threshold) {
                logger.debug('date slicer is recursing by keylist');
                return Promise.resolve(makeKeyList(data))
                    .then((results) => results)
                    .catch((err) => {
                        const errMsg = parseError(err);
                        logger.error('error while subslicing by key', errMsg);
                        return Promise.reject(err);
                    });
            }

            return {
                start: data.start.format(dateFormat),
                end: data.end.format(dateFormat),
                count: data.count
            };
        };
    }

    function awaitChunk(slicerDates: DateConfig, slicerId: number) {
        const shouldDivideByID = opConfig.subslice_by_key;
        const threshold = opConfig.subslice_key_threshold;

        const start = moment(slicerDates.start);
        const end = moment(slicerDates.end);

        const { delayTime, interval } = slicerDates;
        const [step, unit] = interval as ParsedInterval;
        let startPoint = moment(slicerDates.start);
        let limit = moment(slicerDates.end);
        const dateArray: any[] = [];

        const dateParams: DateParams = {
            size: opConfig.size,
            interval: opConfig.interval,
            start,
            end,
            limit
        };

        logger.debug('all date configurations for date slicer', dateParams);

        // set a timer to add the next set it should process
        const injector = setInterval(() => {
            // keep a list of next batches in cases current batch is still running
            dateArray.push({
                startPoint: moment(startPoint).add(step, unit),
                limit: moment(limit).add(step, unit)
            });
        }, delayTime as number);

        events.on('worker:shutdown', () => clearInterval(injector));

        return async function sliceDate(msg: any) {
            if (dateParams.start.isSameOrAfter(limit)) {
                // all done processing current chunk range, check for next range
                if (dateArray.length > 0) {
                    const newRange = dateArray.shift();
                    ({ startPoint, limit } = newRange);
                    // make separate references to prevent mutating both at same time
                    dateParams.start = moment(newRange.startPoint);
                    dateParams.end = moment(newRange.limit);
                } else {
                    return null;
                }
            }
            let data: any;
            try {
                data = await determineSlice(dateParams, slicerId, false);
            } catch (err) {
                const retryInput = dateParams.start.format(dateFormat);
                return retryError(retryInput, err, sliceDate, msg);
            }

            dateParams.start = data.end;
            if (moment(data.end).add(step, unit).isAfter(limit)) {
                dateParams.end = moment(data.end).add(dateParams.limit.diff(data.end));
            } else {
                dateParams.end = moment(data.end).add(step, unit);
            }

            if (shouldDivideByID && data.count >= threshold) {
                logger.debug('date slicer is recursing by keylist');
                return Promise.resolve(makeKeyList(data))
                    .then((results) => results)
                    .catch((err) => {
                        const errMsg = parseError(err);
                        logger.error('error while subslicing by key', errMsg);
                        return Promise.reject(err);
                    });
            }

            return {
                start: data.start.format(dateFormat),
                end: data.end.format(dateFormat),
                count: data.count
            };
        };
    }

    if (executionConfig.lifecycle === 'persistent') {
        return awaitChunk(sliceDates, id);
    }

    return nextChunk(sliceDates, id, retryData);
}
