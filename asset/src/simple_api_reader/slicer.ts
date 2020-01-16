import {
    ParallelSlicer, SlicerFn, WorkerContext, ExecutionConfig, TSError, AnyObject
} from '@terascope/job-components';
import moment from 'moment';
import elasticApi from '@terascope/elasticsearch-api';

import MockedClient from './client';
// @ts-ignore
import DateSlicerFn from '../elasticsearch_reader/elasticsearch_date_range/slicer';
import {
    dateFormat,
    dateFormatSeconds,
    parseDate,
    dateOptions,
    // @ts-ignore
    divideRange,
    processInterval
} from '../__lib';
import { ApiConfig } from './interfaces';
// @ts-ignore


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
            throw new TSError(`Invalid date_field_name: "${this.opConfig.date_field_name}" for index: ${this.opConfig.index}, field was not found on record`);
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

    async updateJob(data: AnyObject) {
        return this.context.apis.executionContext.setMetadata(this.opConfig._op, data);
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
            return processInterval(this.opConfig.interval, this.opConfig.time_resolution, esDates);
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

    isRecoverable() {
        return true;
    }
    // @ts-ignore
    async newSlicer(_id: number): Promise<SlicerFn> {
        // @ts-ignore
        return () => null;
        // const isPersistent = this.executionConfig.lifecycle === 'persistent';
        // const slicerFnArgs: Partial<SlicerArgs> = {
        //     context: this.context,
        //     opConfig: this.opConfig,
        //     executionConfig: this.executionConfig,
        //     logger: this.logger,
        //     id,
        //     api: this.api
        // };

        // await this.api.version();

        // if (this.recoveryData && this.recoveryData.length > 0) {
        //     slicerFnArgs.retryData = this.recoveryData[id];
        // }

        // if (isPersistent) {
        //     const dataIntervals = getTimes(
        //         this.opConfig,
        //         this.executionConfig.slicers,
        //         this.dateFormat
        //     );
        //     slicerFnArgs.dates = dataIntervals[id];
        // } else {
        //     const esDates = await this.getDates();
        //     // query with no results
        //     if (esDates.start == null) {
        //         this.logger.warn(`No data was found in index: ${this.opConfig.index} using query: ${this.opConfig.query}`);
        //         // slicer will run and complete when a null is returned
        //         return async () => null;
        //     }
        //     const interval = await this.getInterval(esDates);
        //     const dateRange = divideRange(
        //         esDates.start,
        //         esDates.limit,
        //         this.executionConfig.slicers,
        //         this.dateFormat
        //     );

        //     await this.updateJob({
        //         start: esDates.start.format(this.dateFormat),
        //         end: esDates.limit.format(this.dateFormat),
        //         interval
        //     });
        //     // we set so auto is replaced with correct interval
        //     slicerFnArgs.opConfig.interval = interval;
        //     slicerFnArgs.dates = dateRange[id];
        // }

        // // @ts-ignore
        // return DateSlicerFn(slicerFnArgs as SlicerArgs);
    }
}
