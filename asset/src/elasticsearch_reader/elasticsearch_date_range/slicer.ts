
import { cloneDeep, toString } from '@terascope/job-components';
import moment from 'moment';
// @ts-ignore
import parseError from '@terascope/error-parser';
import idSlicer from '../../id_reader/id-slicer';
import { SlicerArgs } from '../interfaces';
import * as helpers from '../../../helpers';
import { ESIDSlicerArgs } from '../../id_reader/interfaces';
import { getKeyArray } from '../../id_reader/helpers';

interface SliceResults {
    start: moment.Moment;
    end: moment.Moment;
    count: number;
    key?: string;
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

    function splitTime(start: any, end: any, limit: any) {
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
        dateParams: any, slicerId: number, isExpandedSlice?: boolean, isLimitQuery?: boolean
    ): Promise<SliceResults> {
        let count: number;
        try {
            count = await getCount(dateParams);
        } catch (err) {
            const errMessage = parseError(err);
            logger.error('error with determine slice:', errMessage);
            const error = new Error(`Failure to determine slice: ${toString(err)}`);
            return Promise.reject(error);
        }

        const intervalNum = dateParams.interval[0];
        const intervalUnit = dateParams.interval[1];
        if (count > dateParams.size) {
            // if size is to big after increasing slice, use alternative division behavior
            if (isExpandedSlice) {
            // recurse down to the appropriate size
                const cloneDates: any = {
                    interval: dateParams.interval,
                    limit: dateParams.limit
                };
                const newStart = moment(dateParams.end).subtract(intervalNum, intervalUnit);
                cloneDates.start = newStart;

                // get diff from new start
                const diff = splitTime(cloneDates.start, dateParams.end, dateParams.limit);
                cloneDates.end = moment(cloneDates.start).add(diff, timeResolution);
                // return the zero range start with the correct end

                const data: SliceResults = await determineSlice(cloneDates, slicerId, false);
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

    function getIdData(promiseOfSlicerArray: any) {
        const list: any[] = [];

        return new Promise(((resolve, reject) => {
            promiseOfSlicerArray.then((slicerArray: any[]) => {
                const slicer = slicerArray[0];
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
                            reject(errMessage);
                        });
                }

                iterate();
            });
        }));
    }

    function makeKeyList(data: any) {
        const idConfig = Object.assign({}, opConfig, { starting_key_depth: 0 });
        const idSlicerArs: ESIDSlicerArgs = {
            context,
            opConfig: idConfig,
            executionConfig,
            logger,
            api,
            range: data,
            keySet: getKeyArray(opConfig.key_type)
        };
        const idSlicers = idSlicer(idSlicerArs);

        return getIdData(idSlicers);
    }

    async function getCount(dates: any, key?: string) {
        const end = dates.end ? dates.end : dates.limit;
        const range: any = {
            start: dates.start.format(dateFormat),
            end: end.format(dateFormat)
        };

        if (key) {
            range.key = key;
        }

        const query = api.buildQuery(opConfig, range);
        return api.count(query);
    }

    function nextChunk(dates: any, slicerId: number, retryDataObj: any) {
        const shouldDivideByID = opConfig.subslice_by_key;
        const threshold = opConfig.subslice_key_threshold;
        const intervalNum = opConfig.interval[0];
        const intervalUnit = opConfig.interval[1];
        const dateParams: any = {};

        dateParams.size = opConfig.size;
        dateParams.interval = opConfig.interval;
        dateParams.start = moment(dates.start);

        if (retryDataObj && retryDataObj.lastSlice && retryDataObj.lastSlice.end) {
            dateParams.start = moment(retryDataObj.lastSlice.end);
        }

        dateParams.limit = moment(dates.end);
        dateParams.end = moment(dateParams.start.format(dateFormat)).add(intervalNum, intervalUnit);
        if (dateParams.end.isSameOrAfter(dateParams.limit)) dateParams.end = dateParams.limit;

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
                // @ts-ignore
                dateParams.end = moment(data.end).add(dateParams.limit - data.end);
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

    function awaitChunk(slicerDates: any, slicerId: number) {
        const shouldDivideByID = opConfig.subslice_by_key;
        const threshold = opConfig.subslice_key_threshold;

        const dateParams: any = {};
        dateParams.size = opConfig.size;
        dateParams.start = moment(slicerDates.start);
        dateParams.end = moment(slicerDates.end);

        const { delayTime, interval } = slicerDates;
        let startPoint = moment(slicerDates.start);
        let limit = moment(slicerDates.end);
        const dateArray: any[] = [];

        logger.debug('all date configurations for date slicer', dateParams);

        // set a timer to add the next set it should process
        setInterval(() => {
            // keep a list of next batches in cases current batch is still running
            dateArray.push({
                startPoint: moment(startPoint).add(interval[0], interval[1]),
                limit: moment(limit).add(interval[0], interval[1])
            });
        }, delayTime);

        return async function sliceDate(msg: any) {
            if (dateParams.start.isSameOrAfter(limit)) {
                // all done processing current chunk range, check for next range
                if (dateArray.length > 0) {
                    const newRange = dateArray.shift();
                    ({ startPoint, limit } = newRange);
                    // make separate references to prevent mutating both at same time
                    dateParams.start = moment(newRange.startPoint);
                    dateParams.end = moment(newRange.limit);
                }
                return null;
            }
            let data: any;
            try {
                data = await determineSlice(dateParams, slicerId, false);
            } catch (err) {
                const retryInput = dateParams.start.format(dateFormat);
                return retryError(retryInput, err, sliceDate, msg);
            }

            dateParams.start = data.end;

            if (moment(data.end).add(interval[0], interval[1]).isAfter(limit)) {
                // @ts-ignore
                dateParams.end = moment(data.end).add(limit - data.end);
            } else {
                dateParams.end = moment(data.end).add(interval[0], interval[1]);
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
