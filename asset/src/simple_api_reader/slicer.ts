
import {
    ParallelSlicer, SlicerFn, WorkerContext, ExecutionConfig, TSError
} from '@terascope/job-components';
import moment from 'moment';
import elasticApi from '@terascope/elasticsearch-api';

import MockedClient from './client';
import DateSlicerFn from '../elasticsearch_reader/elasticsearch_date_range/slicer';
import {
    dateFormat,
    dateFormatSeconds,
    parseDate,
    dateOptions,
    divideRange,
    getTimes,
    processInterval
} from '../helpers';
import { ApiConfig } from './interfaces';
import { SlicerArgs } from '../elasticsearch_reader/interfaces';

// TODO: dedup this from ESReader Slicer

export default class ESDateSlicer extends ParallelSlicer<ApiConfig> {
    api: elasticApi.Client;
    dateFormat: string;

    constructor(
        context: WorkerContext,
        opConfig: ApiConfig,
        executionConfig: ExecutionConfig
    ) {
        super(context, opConfig, executionConfig);
        const client = new MockedClient(this.opConfig, this.logger);
        this.api = elasticApi(client, this.logger, this.opConfig);
        const timeResolution = dateOptions(opConfig.time_resolution);
        this.dateFormat = timeResolution === 'ms' ? dateFormat : dateFormatSeconds;
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
            throw new TSError(`date_field_name: "${this.opConfig.date_field_name}" for index: ${this.opConfig.index} does not exist`);
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
        if (!this.opConfig.start || !this.opConfig.end || this.opConfig.interval === 'auto') {
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
            return Promise.resolve(processInterval(
                this.opConfig.time_resolution,
                this.opConfig.interval,
                esDates
            ));
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
        const retryData = this.recoveryData[id];
        const slicerFnArgs: Partial<SlicerArgs> = {
            context: this.context,
            opConfig: this.opConfig,
            executionConfig: this.executionConfig,
            logger: this.logger,
            id,
            api: this.api
        };

        if (isPersistent) {
            const dataIntervals = getTimes(
                this.opConfig,
                this.executionConfig.slicers,
                this.dateFormat
            );
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
                this.executionConfig.slicers,
                this.dateFormat
            );
            this.updateJob(esDates, interval);
            // we set so auto is replaced with correct interval
            slicerFnArgs.opConfig.interval = interval;
            slicerFnArgs.dates = dateRange[id];
            slicerFnArgs.retryData = retryData;
        }

        // @ts-ignore
        return DateSlicerFn(slicerFnArgs as SlicerArgs);
    }
}
