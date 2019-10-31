
import {
    ParallelSlicer, SlicerFn, SlicerRecoveryData
} from '@terascope/job-components';
import moment from 'moment';
import elasticApi from '@terascope/elasticsearch-api';
// @ts-ignore TODO: check for types
import dateMath from 'datemath-parser';
import MockedClient from './client';
import DateSlicerFn from '../elasticsearch_reader/elasticsearch_date_range/slicer';
import { dateOptions, dateFormat } from '../../helpers';
import { ApiConfig } from './interfaces';
import { SlicerArgs } from '../elasticsearch_reader/interfaces';

// TODO: dedup this from ESReader Slicer

export default class ESDateSlicer extends ParallelSlicer<ApiConfig> {
    api!: elasticApi.Client;
    retryDataArray!: any;

    async initialize(recoveryData: SlicerRecoveryData[]) {
        const client = new MockedClient(this.opConfig, this.logger);
        this.api = elasticApi(client, this.logger, this.opConfig);
        this.retryDataArray = recoveryData;
    }

    async getDates() {
        const [startDate, endDate] = await Promise.all([this.getIndexDate(this.opConfig.start, 'start'), this.getIndexDate(this.opConfig.end, 'end')]);
        const finalDates = { start: startDate, limit: endDate };
        if (startDate && endDate) {
            this.logger.info(`execution: ${this.executionConfig.ex_id} start and end range times are ${startDate.format(dateFormat)} and ${endDate.format(dateFormat)}`);
        }
        return finalDates;
    }

    async getIndexDate(date: null | string, order: string) {
        const sortObj = {};
        let givenDate: any = null;
        let query: any = null;

        if (date) {
            givenDate = parseDate(date);
            query = this.api.buildQuery(
                this.opConfig,
                { count: 1, start: this.opConfig.start, end: this.opConfig.end }
            );
        } else {
            const sortOrder = order === 'start' ? 'asc' : 'desc';

            sortObj[this.opConfig.date_field_name] = { order: sortOrder };

            query = {
                index: this.opConfig.index,
                size: 1,
                body: {
                    sort: [
                        sortObj
                    ]
                }
            };

            if (this.opConfig.query) {
                query.q = this.opConfig.query;
            }
        }

        // using this query to catch potential errors even if a date is given already
        const results = await this.api.search(query);
        const data = results[0];
        if (data == null) {
            this.logger.warn(`no data was found using query ${JSON.stringify(query)} for index: ${this.opConfig.index}`);
            return null;
        }

        if (data[this.opConfig.date_field_name] == null) {
            throw new Error(`date_field_name: "${this.opConfig.date_field_name}" for index: ${this.opConfig.index} does not exist, data: ${JSON.stringify(data)}, results: ${JSON.stringify(results)}`);
        }

        if (givenDate) {
            return givenDate;
        }

        if (order === 'start') {
            return parseDate(data[this.opConfig.date_field_name]);
        }
        // end date is non-inclusive, adding 1s so range will cover it
        const newDate = data[this.opConfig.date_field_name];
        // @ts-ignore
        const time = moment(newDate).add(1, this.opConfig.time_resolution);
        return parseDate(time.format(dateFormat));
    }

    updateJob(dates: any, interval: any) {
        const opName = this.opConfig._op;

        // this sends actual dates to execution context so that it can keep
        // track of them for recoveries
        if (!this.opConfig.start || !this.opConfig.end) {
            const { operations } = this.executionConfig;
            const opIndex = operations.findIndex((config) => config._op === opName);
            const update = {
                start: dates.start.format(dateFormat),
                end: dates.limit.format(dateFormat)
            };

            const updatedOpConfig = Object.assign({}, this.opConfig, update, { interval });
            operations[opIndex] = updatedOpConfig;
            this.events.emit('slicer:execution:update', { update: operations });
        }
    }

    async getCount(dates: any, key?: string) {
        const end = dates.end ? dates.end : dates.limit;
        const range: any = {
            start: dates.start.format(dateFormat),
            end: end.format(dateFormat)
        };

        if (key) {
            range.key = key;
        }

        const query = this.api.buildQuery(this.opConfig, range);
        return this.api.count(query);
    }

    async getInterval(esDates: any) {
        if (this.opConfig.interval !== 'auto') {
            return Promise.resolve(processInterval(this.opConfig.interval, esDates));
        }

        const count = await this.getCount(esDates);
        const numOfSlices = Math.ceil(count / this.opConfig.size);
        const timeRangeMilliseconds = esDates.limit.diff(esDates.start);
        const millisecondInterval = Math.floor(timeRangeMilliseconds / numOfSlices);

        if (this.opConfig.time_resolution === 's') {
            let seconds = Math.floor(millisecondInterval / 1000);
            if (seconds < 1) {
                seconds = 1;
            }
            return [seconds, 's'];
        }

        return [millisecondInterval, 'ms'];
    }

    async newSlicer(id: number): Promise<SlicerFn> {
        const isPersistent = this.executionConfig.lifecycle === 'persistent';
        const retryData = this.retryDataArray[id];
        const slicerFnArgs: Partial<SlicerArgs> = {
            context: this.context,
            opConfig: this.opConfig,
            executionConfig: this.executionConfig,
            logger: this.logger,
            id,
            api: this.api
        };

        if (isPersistent) {
            const dataIntervals = getTimes(this.opConfig, this.executionConfig.slicers);
            slicerFnArgs.dates = dataIntervals[id];
        } else {
            await this.api.version();
            const esDates = await this.getDates();
            // query with no results
            if (esDates.start == null) {
                this.logger.warn(`No data was found in index: ${this.opConfig.index} using query: ${this.opConfig.query}`);
                // slicer will run and complete when a null is returned
                return async () => null;
            }
            const interval = await this.getInterval(esDates);
            const dateRange = divideRange(
                esDates.start,
                esDates.limit,
                this.executionConfig.slicers
            );
            this.updateJob(esDates, interval);
            slicerFnArgs.dates = dateRange[id];
            slicerFnArgs.retryData = retryData;
        }

        // @ts-ignore
        return DateSlicerFn(slicerFnArgs as SlicerArgs);
    }
}

function parseDate(date: string) {
    let result;

    if (moment(new Date(date)).isValid()) {
        result = moment(new Date(date));
    } else {
        const ms = dateMath.parse(date);
        result = moment(ms);
    }

    return result;
}

function getTimes(opConfig: ApiConfig, numOfSlicers: number) {
    const end = processInterval(opConfig.time_resolution, opConfig.interval);
    const delayInterval = processInterval(opConfig.time_resolution, opConfig.delay);
    const delayTime = getMilliseconds(end);
    const delayedEnd = moment().subtract(delayInterval[0], delayInterval[1]).format(dateFormat);
    const delayedStart = moment(delayedEnd).subtract(end[0], end[1]).format(dateFormat);
    const dateArray = divideRange(delayedStart, delayedEnd, numOfSlicers);

    return dateArray.map((dates: any) => {
        dates.delayTime = delayTime;
        dates.interval = end;
        return dates;
    });
}

function getMilliseconds(interval: any[]) {
    const times = {
        d: 86400000,
        h: 3600000,
        m: 60000,
        s: 1000,
        ms: 1
    };

    return interval[0] * times[interval[1]];
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

function processInterval(timeResolution: string, str: string, esDates?: any) {
    if (!moment(new Date(str)).isValid()) {
        // one or more digits, followed by one or more letters, case-insensitive
        const regex = /(\d+)(\D+)/i;
        const interval = regex.exec(str);
        if (interval === null) {
            throw new Error('elasticsearch_reader interval and/or delay are incorrectly formatted. Needs to follow [number][letter\'s] format, e.g. "12s"');
        }

        // dont need first parameter, its the full string
        interval.shift();
        interval[1] = dateOptions(interval[1]);
        return compareInterval(interval, esDates, timeResolution);
    }

    throw new Error('elasticsearch_reader interval and/or delay are incorrectly formatted. Needs to follow [number][letter\'s] format, e.g. "12s"');
}


function divideRange(start: any, end: any, numOfSlicers: number) {
    const results = [];
    const startNum = Number(moment(start).format('x'));
    const endNum = Number(moment(end).format('x'));
    const range = (endNum - startNum) / numOfSlicers;

    const step = moment(start);

    for (let i = 0; i < numOfSlicers; i += 1) {
        const rangeObj = {
            start: step.format(dateFormat),
            end: step.add(range).format(dateFormat)
        };
        results.push(rangeObj);
    }

    // make sure that end of last segment is always correct
    const endingDate = end.format ? end.format(dateFormat) : moment(end).format(dateFormat);
    results[results.length - 1].end = endingDate;
    return results;
}
