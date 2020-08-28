import {
    ParallelSlicer,
    SlicerFn,
    WorkerContext,
    ExecutionConfig,
    TSError,
    AnyObject,
    isFunction,
    SlicerRecoveryData,
} from '@terascope/job-components';
import moment from 'moment';
import dateSlicerFn from './slicer-fn';
import Reader from '../../elasticsearch_reader_api/reader';
import {
    processInterval,
    dateFormat,
    dateOptions,
    dateFormatSeconds,
    parseDate,
    determineStartingPoint,
    delayedStreamSegment,
    buildQuery
} from './helpers';
import {
    ESDateConfig,
    SlicerArgs,
    DateSegments,
    StartPointConfig,
    SlicerDateResults,
} from '../interfaces';
import WindowState from '../window-state';

import { ElasticReaderFactoryAPI } from '../../elasticsearch_reader_api/interfaces';

type FetchDate = moment.Moment | null;

export default class DateSlicer extends ParallelSlicer<ESDateConfig> {
    api!: Reader;
    dateFormat: string;
    windowState: WindowState
    startTime = moment.utc();

    constructor(
        context: WorkerContext,
        opConfig: ESDateConfig,
        executionConfig: ExecutionConfig,
    ) {
        super(context, opConfig, executionConfig);
        const timeResolution = dateOptions(opConfig.time_resolution);
        this.dateFormat = timeResolution === 'ms' ? dateFormat : dateFormatSeconds;
        this.windowState = new WindowState(executionConfig.slicers);
    }

    async initialize(recoveryData: SlicerRecoveryData[]): Promise<void> {
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {});

        // NOTE ORDER MATTERS
        // a parallel slicer initialize calls newSlicer multiple times
        // need to make api before newSlicer is called

        await super.initialize(recoveryData);
    }

    async getDates(): Promise<{ start: FetchDate; limit: FetchDate; }> {
        const [startDate, endDate] = await Promise.all([this.getIndexDate(this.opConfig.start, 'start'), this.getIndexDate(this.opConfig.end, 'end')]);
        const finalDates = { start: startDate, limit: endDate };
        if (startDate && endDate) {
            this.logger.info(`execution: ${this.executionConfig.ex_id} start and end range times are ${startDate.format(this.dateFormat)} and ${endDate.format(this.dateFormat)}`);
        }
        return finalDates;
    }

    async getIndexDate(date: null | string, order: string): Promise<FetchDate> {
        // we have a date, parse and return it
        if (date) return parseDate(date);
        // we are in auto, so we determine each part
        const sortObj = {};
        const sortOrder = order === 'start' ? 'asc' : 'desc';

        sortObj[this.opConfig.date_field_name] = { order: sortOrder };

        const query: AnyObject = {
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

        // using this query to catch potential errors even if a date is given already
        const results = await this.api.client.search(query);
        const [data] = results;

        if (data == null) {
            this.logger.warn(`no data was found using query ${JSON.stringify(query)} for index: ${this.opConfig.index}`);
            return null;
        }

        if (data[this.opConfig.date_field_name] == null) {
            throw new TSError(`Invalid date_field_name: "${this.opConfig.date_field_name}" for index: ${this.opConfig.index}, field does not exist on record`);
        }

        if (order === 'start') {
            return parseDate(data[this.opConfig.date_field_name]);
        }
        // end date is non-inclusive, adding 1s so range will cover it
        const newDate = data[this.opConfig.date_field_name];
        const time = moment.utc(newDate).add(1, this.opConfig.time_resolution);
        const parsedDate = parseDate(time.format(this.dateFormat));

        return parsedDate;
    }

    async updateJob(data: AnyObject): Promise<void> {
        const { setMetadata } = this.context.apis.executionContext;

        if (setMetadata && isFunction(setMetadata)) {
            return this.context.apis.executionContext.setMetadata(this.opConfig._op, data);
        }
    }

    async getCount(dates: DateSegments, key?: string): Promise<number> {
        const end = dates.limit;
        const range: any = {
            start: dates.start.format(this.dateFormat),
            end: end.format(this.dateFormat)
        };

        if (key) {
            range.key = key;
        }

        const query = buildQuery(this.opConfig, range);
        // TODO: review types here
        return this.api.count(query as any);
    }

    async getInterval(
        interval: string, esDates?: DateSegments
    ): Promise<[number, moment.unitOfTime.Base]> {
        if (this.opConfig.interval !== 'auto') {
            return processInterval(interval, this.opConfig.time_resolution, esDates);
        }
        if (esDates == null) throw new Error('must provide dates to create interval');

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

    isRecoverable(): boolean {
        return true;
    }

    async newSlicer(id: number): Promise<SlicerFn> {
        const isPersistent = this.executionConfig.lifecycle === 'persistent';
        const { startTime } = this;
        const slicerFnArgs: Partial<SlicerArgs> = {
            opConfig: this.opConfig,
            executionConfig: this.executionConfig,
            logger: this.logger,
            id,
            api: this.api,
            events: this.context.apis.foundation.getSystemEvents()
        };

        await this.api.client.version();

        const recoveryData = this.recoveryData.map(
            (slice) => slice.lastSlice
        ).filter(Boolean) as SlicerDateResults[];

        if (isPersistent) {
            // we need to interval to get starting dates
            const [interval, latencyInterval] = await Promise.all([
                this.getInterval(this.opConfig.interval),
                this.getInterval(this.opConfig.delay)
            ]);

            slicerFnArgs.interval = interval;
            slicerFnArgs.latencyInterval = latencyInterval;
            slicerFnArgs.windowState = this.windowState;

            const { start, limit } = delayedStreamSegment(startTime, interval, latencyInterval);

            const config: StartPointConfig = {
                dates: { start, limit },
                id,
                numOfSlicers: this.executionConfig.slicers,
                recoveryData,
                interval
            };
            const { dates, range } = await determineStartingPoint(config);

            slicerFnArgs.dates = dates;
            slicerFnArgs.primaryRange = range;
        } else {
            const esDates = await this.getDates();
            // query with no results
            if (esDates.start == null || esDates.limit == null) {
                this.logger.warn(`No data was found in index: ${this.opConfig.index} using query: ${this.opConfig.query}`);
                // slicer will run and complete when a null is returned
                return async () => null;
            }

            const interval = await this.getInterval(
                this.opConfig.interval,
                esDates as DateSegments
            );
            slicerFnArgs.interval = interval;

            await this.updateJob({
                start: moment(esDates.start.format(this.dateFormat)).toISOString(),
                end: moment(esDates.limit.format(this.dateFormat)).toISOString(),
                interval
            });

            const config: StartPointConfig = {
                dates: esDates as DateSegments,
                id,
                numOfSlicers: this.executionConfig.slicers,
                recoveryData,
                interval
            };
            // we do not care for range for once jobs
            const { dates } = await determineStartingPoint(config);
            slicerFnArgs.dates = dates;
        }

        return dateSlicerFn(slicerFnArgs as SlicerArgs) as SlicerFn;
    }
}
