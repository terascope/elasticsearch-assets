import {
    ParallelSlicer,
    SlicerFn,
    getClient,
    WorkerContext,
    ExecutionConfig,
    TSError,
    AnyObject
} from '@terascope/job-components';
import moment from 'moment';
import elasticApi from '@terascope/elasticsearch-api';
import dateSlicerFn from './elasticsearch_date_range/slicer';
import {
    processInterval,
    dateFormat,
    dateOptions,
    dateFormatSeconds,
    parseDate,
    getMilliseconds,
    determineStartingPoint
} from '../helpers';
import {
    ESReaderConfig,
    SlicerArgs,
    DateSegments,
    StartPointConfig,
    SlicerDateResults
} from './interfaces';

type FetchDate = moment.Moment | null;

export default class ESDateSlicer extends ParallelSlicer<ESReaderConfig> {
    api: elasticApi.Client;
    dateFormat: string;

    constructor(
        context: WorkerContext,
        opConfig: ESReaderConfig,
        executionConfig: ExecutionConfig
    ) {
        super(context, opConfig, executionConfig);
        const client = getClient(this.context, this.opConfig, 'elasticsearch');
        this.api = elasticApi(client, this.logger, this.opConfig);
        const timeResolution = dateOptions(opConfig.time_resolution);
        this.dateFormat = timeResolution === 'ms' ? dateFormat : dateFormatSeconds;
    }

    async getDates() {
        const [startDate, endDate] = await Promise.all([this.getIndexDate(this.opConfig.start, 'start'), this.getIndexDate(this.opConfig.end, 'end')]);
        const finalDates = { start: startDate, limit: endDate };
        if (startDate && endDate) {
            this.logger.info(`execution: ${this.executionConfig.ex_id} start and end range times are ${startDate.format(this.dateFormat)} and ${endDate.format(this.dateFormat)}`);
        }
        return finalDates;
    }

    async getIndexDate(date: null | string, order: string): Promise<FetchDate> {
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
        const [data] = results;

        if (data == null) {
            this.logger.warn(`no data was found using query ${JSON.stringify(query)} for index: ${this.opConfig.index}`);
            return null;
        }

        if (data[this.opConfig.date_field_name] == null) {
            throw new TSError(`Invalid date_field_name: "${this.opConfig.date_field_name}" for index: ${this.opConfig.index}, field does not exist on record`);
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
        return parseDate(time.format(this.dateFormat));
    }

    async updateJob(data: AnyObject) {
        return this.context.apis.executionContext.setMetadata(this.opConfig._op, data);
    }

    async getCount(dates: any, key?: string) {
        const end = dates.end ? dates.end : dates.limit;
        const range: any = {
            start: dates.start.format(this.dateFormat),
            end: end.format(this.dateFormat)
        };

        if (key) {
            range.key = key;
        }

        const query = this.api.buildQuery(this.opConfig, range);
        return this.api.count(query);
    }

    async getInterval(interval: string, esDates?: DateSegments) {
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

    isRecoverable() {
        return true;
    }

    async newSlicer(id: number): Promise<SlicerFn> {
        const isPersistent = this.executionConfig.lifecycle === 'persistent';
        const slicerFnArgs: Partial<SlicerArgs> = {
            context: this.context,
            opConfig: this.opConfig,
            executionConfig: this.executionConfig,
            logger: this.logger,
            id,
            api: this.api
        };

        await this.api.version();

        const recoveryData = this.recoveryData.map(
            (slice) => slice.lastSlice
        ) as SlicerDateResults[];

        if (isPersistent) {
            // we need to interval to get starting dates
            const [interval, delayInterval] = await Promise.all([
                this.getInterval(this.opConfig.interval),
                this.getInterval(this.opConfig.delay)
            ]);

            const delayTime = getMilliseconds(interval);

            slicerFnArgs.interval = interval;
            slicerFnArgs.delayTime = delayTime;

            const delayedLimit = moment().subtract(
                delayInterval[0],
                delayInterval[1]
            );

            const delayedStart = moment(delayedLimit).subtract(
                interval[0],
                interval[1]
            );

            const config: StartPointConfig = {
                dates: { start: delayedStart, limit: delayedLimit },
                id,
                numOfSlicers: this.executionConfig.slicers,
                recoveryData,
                interval
            };

            slicerFnArgs.dates = await determineStartingPoint(config);
        } else {
            const esDates = await this.getDates();
            // query with no results
            if (esDates.start == null || esDates.limit == null) {
                this.logger.warn(`No data was found in index: ${this.opConfig.index} using query: ${this.opConfig.query}`);
                // slicer will run and complete when a null is returned
                return async () => null;
            }
            // @ts-ignore TODO: fixme:
            const interval = await this.getInterval(
                this.opConfig.interval,
                esDates as DateSegments
            );
            slicerFnArgs.interval = interval;

            await this.updateJob({
                start: esDates.start.format(this.dateFormat),
                end: esDates.limit.format(this.dateFormat),
                interval
            });

            const config: StartPointConfig = {
                // @ts-ignore FIXME:
                dates: esDates,
                id,
                numOfSlicers: this.executionConfig.slicers,
                recoveryData,
                interval
            };

            slicerFnArgs.dates = await determineStartingPoint(config);
        }

        return dateSlicerFn(slicerFnArgs as SlicerArgs) as SlicerFn;
    }
}
