import type { SlicerFn } from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import { EventEmitter } from 'events';
import {
    AnyObject,
    DataEntity,
    isObjectEntity,
    getTypeOf,
    Logger,
    toNumber,
    TSError,
    isSimpleObject,
    isNumber,
    isValidDate,
    isFunction,
    isString,
    get
} from '@terascope/utils';
import { DataFrame } from '@terascope/data-mate';
import { DataTypeConfig } from '@terascope/data-types';
import moment from 'moment';
import { CountParams, SearchParams, Client } from 'elasticsearch';
import dateSlicerFn from '../elasticsearch-date-slicer';
import idSlicerFn from '../elasticsearch-id-slicer';
import { getKeyArray } from '../elasticsearch-id-slicer/helpers';

import {
    buildQuery,
    dateOptions,
    processInterval,
    dateFormat,
    dateFormatSeconds,
    parseDate,
    determineStartingPoint,
    delayedStreamSegment
} from '../elasticsearch-date-slicer/helpers';
import {
    ESReaderOptions,
    SlicerDateResults,
    DateSegments,
    InputDateSegments,
    SlicerArgs,
    StartPointConfig,
    IDType,
    DateSlicerArgs,
    DateSlicerConfig,
    IDSlicerArgs,
    IDSlicerConfig
} from '../interfaces';
import SpacesClient from '../spaces-api/spaces-client';
import { WindowState } from '../window-state';
import { createBulkSenderAPI, ElasticsearchSenderConfig } from '../elasticsearch-bulk-sender';
import { ElasticsearchSender } from '../elasticsearch-bulk-sender/bulk-sender';

type ReaderClient = Client | SpacesClient
type FetchDate = moment.Moment | null;

function isValidDataTypeConfig(record: any): record is DataTypeConfig {
    if (!record || !isSimpleObject(record)) return false;
    if (!record.version || !isNumber(record.version)) return false;
    if (!record.fields || !isSimpleObject(record.fields)) return false;

    return true;
}

export class BaseReaderAPI {
    readonly config: ESReaderOptions;
    logger: Logger;
    private _baseClient: AnyObject;
    protected readonly client: elasticAPI.Client;
    private hasDefaultQueries = false;
    private windowSize: undefined | number = undefined;
    protected readonly dateFormat: string;
    protected readonly emitter: EventEmitter;

    constructor(
        config: ESReaderOptions, client: ReaderClient, emitter: EventEmitter, logger: Logger
    ) {
        const { connection, index, time_resolution } = config;
        const clientConfig = {
            full_response: false,
            connection,
            index
        };

        if (config.use_data_frames) {
            clientConfig.full_response = true;
            if (!isValidDataTypeConfig(config.type_config)) throw new Error('Parameter "type_config" must be set if DataFrames are being returned');
        }

        this.config = Object.freeze(config);
        this.emitter = emitter;
        this.logger = logger;
        this._baseClient = client;
        this.client = elasticAPI(client, logger, clientConfig);
        if (config.query || config.geo_field) this.hasDefaultQueries = true;
        const timeResolution = time_resolution ? dateOptions(time_resolution) : '';
        this.dateFormat = timeResolution === 'ms' ? dateFormat : dateFormatSeconds;
    }

    makeWindowState(numOfSlicers: number): WindowState {
        return new WindowState(numOfSlicers);
    }

    async count(queryParams: Partial<SlicerDateResults> = {}): Promise<number> {
        const query = buildQuery(this.config, queryParams);
        return this.client.count(query as CountParams);
    }

    async fetch(queryParams: Partial<SlicerDateResults> = {}): Promise<DataEntity[]|DataFrame> {
        // attempt to get window if not set
        if (!this.windowSize) {
            const size = await this.getWindowSize();
            if (size) this.windowSize = size;
        }
        // if we did go ahead and complete query
        if (this.windowSize) {
            const query = buildQuery(this.config, queryParams);
            query.size = this.windowSize;
            const start = Date.now();
            // this could be a full  ES request, Spaces Request, or an array of data-entities
            const searchResults = await this._searchRequest(query) as any;

            const searchEnd = Date.now();

            if (this.config.use_data_frames) {
                const typeConfig = this.config.type_config as DataTypeConfig;
                const records = searchResults.hits.hits.map((data: AnyObject) => data._source);
                const metrics: AnyObject = {
                    search_time: searchEnd - start,
                    fetched: records.length,
                    total: searchResults.hits.total
                };

                // we do not have access to complexity right now
                return DataFrame.fromJSON(
                    typeConfig,
                    records,
                    {
                        name: '<unknown>',
                        metadata: {
                            search_end_time: searchEnd,
                            metrics
                        }
                    }
                );
            }

            return searchResults;
        }

        // index is not up, return empty, we log in getWindowSize
        return [];
    }

    async _searchRequest(query: SearchParams): Promise<DataEntity[]> {
        return this.client.search(query);
    }

    makeBulkSender(bulkConfig: Partial<ElasticsearchSenderConfig> = {}): ElasticsearchSender {
        const { client } = this;
        const { index, connection, size } = this.config;
        const config = {
            index,
            connection,
            size,
            ...bulkConfig
        };

        return createBulkSenderAPI({ client, config });
    }

    async determineSliceInterval(
        interval: string, esDates?: InputDateSegments
    ): Promise<[number, moment.unitOfTime.Base]> {
        if (this.config.interval !== 'auto') {
            return processInterval(interval, this.config.time_resolution, esDates);
        }

        if (esDates == null) throw new Error('must provide dates to create interval');

        const count = await this.count({
            start: moment(esDates.start).format(this.dateFormat),
            end: moment(esDates.limit).format(this.dateFormat),
        });

        const numOfSlices = Math.ceil(count / this.config.size);
        const timeRangeMilliseconds = moment(esDates.limit).diff(esDates.start);
        const millisecondInterval = Math.floor(timeRangeMilliseconds / numOfSlices);

        if (this.config.time_resolution === 's') {
            let seconds = Math.floor(millisecondInterval / 1000);
            if (seconds < 1) seconds = 1;
            return [seconds, 's'];
        }

        return [millisecondInterval, 'ms'];
    }

    private validateIDSlicerConfig(input: unknown): IDSlicerConfig {
        if (isObject(input)) {
            if (!isNumber(input.slicerID)) throw new Error(`Parameter slicerID must be a number, got ${getTypeOf(input.slicerID)}`);
            if (!isNumber(input.numOfSlicers)) throw new Error(`Parameter numOfSlicers must be a number, got ${getTypeOf(input.numOfSlicers)}`);
            if (this.version >= 6 && (!isString(input.idFieldName) || input.idFieldName.length === 0)) throw new Error(`Parameter idFieldName must be a string, got ${getTypeOf(input.idFieldName)}`);

            if (input.recoveryData) {
                if (Array.isArray(input.recoveryData)) {
                    const areAllObjects = input.recoveryData.every((val) => isSimpleObject(val));
                    if (!areAllObjects) throw new Error('Input recoveryData must be an array of recovered slices, cannot have mixed values');
                } else {
                    throw new Error(`Input recoveryData must be an array of recovered slices, got ${getTypeOf(input.recoveryData)}`);
                }
            } else {
                input.recoveryData = [];
            }

            if (!input.keyType || !Object.values(IDType).includes(input.keyType)) throw new Error(`Invalid parameter key_type, got ${input.keyType}`);
            if (input.keyRange) {
                if (input.keyRange.length === 0) throw new Error('Invalid key_range parameter, must be an array with length > 0');
                if (!input.keyRange.every(isString)) throw new Error('Invalid key_range parameter, must be an array of strings');
            }
        } else {
            throw new Error(`Input must be an object, received ${getTypeOf(input)}`);
        }

        return input as unknown as IDSlicerConfig;
    }

    async makeIDSlicer(args: IDSlicerConfig): Promise<SlicerFn> {
        const config = this.validateIDSlicerConfig(args);
        const countFn = this.count.bind(this);

        const {
            numOfSlicers,
            slicerID,
            keyRange,
            keyType,
            recoveryData,
            startingKeyDepth,
            idFieldName
        } = config;

        const baseKeyArray = getKeyArray(keyType);
        // we slice as not to mutate for when this is called again
        const keyArray = keyRange ? keyRange.slice() : baseKeyArray.slice();

        if (difference(keyArray, baseKeyArray).length > 0) {
            const error = new TSError(`key_range specified for id_reader contains keys not found in: ${keyType}`);
            return Promise.reject(error);
        }

        const keySet = divideKeyArray(keyArray, numOfSlicers);
        const { type, size } = this.config;

        const slicerConfig: IDSlicerArgs = {
            events: this.emitter,
            logger: this.logger,
            keySet: keySet[slicerID],
            version: this.version,
            baseKeyArray,
            startingKeyDepth,
            countFn,
            type,
            idFieldName,
            size
        };

        if (recoveryData && recoveryData.length > 0) {
            // TODO: verify what retryData is
            // real retry of executionContext here, need to reformat retry data
            const parsedRetry = recoveryData.map((obj: any) => {
                // regex to get str between # and *
                if (obj.lastSlice) {
                    if (this.version <= 5) return obj.lastSlice.key.match(/#(.*)\*/)[1];
                    const { value } = obj.lastSlice.wildcard;
                    // get rid of the * char which is at the end of the string
                    return value.slice(0, value.length - 1);
                }
                // some slicers might not have a previous slice, need to start from scratch
                return '';
            })[slicerID];

            slicerConfig.retryData = parsedRetry;
        }

        return idSlicerFn(slicerConfig as IDSlicerArgs);
    }

    private validateDateSlicerConfig(input: unknown): DateSlicerConfig {
        if (isObject(input)) {
            if (!(input.lifecycle === 'once' || input.lifecycle === 'persistent')) throw new Error('Parameter lifecycle must be set to "once" or "persistent"');
            if (!isNumber(input.slicerID)) throw new Error(`Parameter slicerID must be a number, got ${getTypeOf(input.slicerID)}`);
            if (!isNumber(input.numOfSlicers)) throw new Error(`Parameter numOfSlicers must be a number, got ${getTypeOf(input.numOfSlicers)}`);

            if (input.recoveryData) {
                if (Array.isArray(input.recoveryData)) {
                    const areAllObjects = input.recoveryData.every((val) => isSimpleObject(val));
                    if (!areAllObjects) throw new Error('Input recoveryData must be an array of recovered slices, cannot have mixed values');
                } else {
                    throw new Error(`Input recoveryData must be an array of recovered slices, got ${getTypeOf(input.recoveryData)}`);
                }
            } else {
                input.recoveryData = [];
            }

            if (input.lifecycle === 'persistent') {
                if (!input.windowState || !input.windowState.checkin) throw new Error(`Invalid parameter windowState, must provide a valid windowState in "persistent" mode, got ${getTypeOf(input.windowState)}`);
                if (!input.startTime || !isValidDate(input.startTime)) throw new Error(`Invalid parameter startTime, must provide a valid date in "persistent" mode, got ${getTypeOf(input.startTime)}`);
            }

            if (input.hook && !isFunction(input.hook)) throw new Error('Input hook must be a function if provided');
        } else {
            throw new Error(`Input must be an object, received ${getTypeOf(input)}`);
        }

        return input as unknown as DateSlicerConfig;
    }

    async makeDateSlicer(args: DateSlicerArgs): Promise<SlicerFn> {
        const config = this.validateDateSlicerConfig(args);
        const {
            slicerID,
            lifecycle,
            numOfSlicers,
            windowState,
        } = config;

        const isPersistent = lifecycle === 'persistent';
        const countFn = this.count.bind(this);

        const slicerFnArgs: Partial<SlicerArgs> = {
            opConfig: this.config,
            lifecycle,
            numOfSlicers,
            logger: this.logger,
            id: slicerID,
            events: this.emitter,
            version: this.version,
            countFn,
            windowState
        };

        await this.verifyIndex();

        const recoveryData = config.recoveryData.map(
            (slice) => slice.lastSlice
        ).filter(Boolean) as SlicerDateResults[];

        if (isPersistent) {
            // we need to interval to get starting dates
            const [interval, latencyInterval] = await Promise.all([
                this.determineSliceInterval(this.config.interval),
                this.determineSliceInterval(this.config.delay)
            ]);

            slicerFnArgs.interval = interval;
            slicerFnArgs.latencyInterval = latencyInterval;
            slicerFnArgs.windowState = config.windowState as WindowState;

            const { start, limit } = delayedStreamSegment(
                config.startTime as string,
                interval,
                latencyInterval
            );

            const startConfig: StartPointConfig = {
                dates: { start, limit },
                id: slicerID,
                numOfSlicers,
                recoveryData,
                interval
            };
            const { dates, range } = await determineStartingPoint(startConfig);

            slicerFnArgs.dates = dates;
            slicerFnArgs.primaryRange = range;
        } else {
            const esDates = await this.determineDateRanges();
            // query with no results
            if (esDates.start == null || esDates.limit == null) {
                this.logger.warn(`No data was found in index: ${this.config.index} using query: ${this.config.query}`);
                // slicer will run and complete when a null is returned
                return async () => null;
            }
            // TODO: we might want to consider making an interval for each slicer range
            const interval = await this.determineSliceInterval(
                this.config.interval,
                esDates as DateSegments
            );
            slicerFnArgs.interval = interval;

            const startConfig: StartPointConfig = {
                dates: esDates as DateSegments,
                id: slicerID,
                numOfSlicers,
                recoveryData,
                interval
            };

            if (config.hook) {
                const params = {
                    interval,
                    start: moment(esDates.start.format(this.dateFormat)).toISOString(),
                    end: moment(esDates.limit.format(this.dateFormat)).toISOString(),
                };
                await config.hook(params);
            }
            // we do not care for range for once jobs
            const { dates } = await determineStartingPoint(startConfig);
            slicerFnArgs.dates = dates;
        }

        return dateSlicerFn(slicerFnArgs as SlicerArgs) as SlicerFn;
    }

    async determineDateRanges(): Promise<{ start: FetchDate; limit: FetchDate; }> {
        const [startDate, endDate] = await Promise.all([this.getIndexDate(this.config.start, 'start'), this.getIndexDate(this.config.end, 'end')]);
        const finalDates = { start: startDate, limit: endDate };
        return finalDates;
    }

    private async getIndexDate(date: null | string, order: string): Promise<FetchDate> {
        // we have a date, parse and return it
        if (date) return parseDate(date);
        // we are in auto, so we determine each part
        const sortObj = {};
        const sortOrder = order === 'start' ? 'asc' : 'desc';

        sortObj[this.config.date_field_name] = { order: sortOrder };

        const query: AnyObject = {
            index: this.config.index,
            size: 1,
            body: {
                sort: [
                    sortObj
                ]
            }
        };

        if (this.config.query) {
            query.q = this.config.query;
        }

        // using this query to catch potential errors even if a date is given already
        const results = await this.client.search(query);
        const data = get(results, 'hits.hits[0]._source', results[0]);

        if (data == null) {
            this.logger.warn(`no data was found using query ${JSON.stringify(query)} for index: ${this.config.index}`);
            return null;
        }

        if (data[this.config.date_field_name] == null) {
            throw new TSError(`Invalid date_field_name: "${this.config.date_field_name}" for index: ${this.config.index}, field does not exist on record`);
        }

        if (order === 'start') {
            return parseDate(data[this.config.date_field_name]);
        }
        // end date is non-inclusive, adding 1s so range will cover it
        const newDate = data[this.config.date_field_name];
        const time = moment(newDate).add(1, this.config.time_resolution);
        const parsedDate = parseDate(time.format(this.dateFormat));

        return parsedDate;
    }

    private async getSettings(query: AnyObject): Promise<AnyObject | null> {
        try {
            return this._baseClient.indices.getSettings(query);
        } catch (_err) {
            this.logger.warn(`index: ${this.config.index} is not yet created`);
            return null;
        }
    }

    async getWindowSize(): Promise<number | null> {
        const window = 'index.max_result_window';
        const { index } = this.config;

        const query = {
            index,
            flat_settings: true,
            include_defaults: true,
            allow_no_indices: true
        };

        const settings = await this.getSettings(query);

        if (settings) {
            for (const [key, configs] of Object.entries(settings)) {
                if (key.match(index)) {
                    const defaultPath = configs.defaults[window];
                    const configPath = configs.settings[window];
                    // config goes first as it overrides an defaults
                    if (configPath) return toNumber(configPath);
                    if (defaultPath) return toNumber(defaultPath);
                }
            }
        }

        return null;
    }

    get version(): number {
        return this.client.getESVersion();
    }

    async verifyIndex(): Promise<boolean|undefined> {
        // this is method in api is badly named
        return this.client.version();
    }
}

function isObject(val: unknown): val is AnyObject {
    return isObjectEntity(val);
}

function difference(srcArray: string[] | null, valArray: string[]) {
    const results: string[] = [];
    if (!srcArray) return results;

    for (const val of srcArray) {
        if (!valArray.includes(val)) {
            results.push(val);
        }
    }
    return results;
}

function divideKeyArray(keysArray: string[], num: number) {
    const results = [];
    const len = num;

    for (let i = 0; i < len; i += 1) {
        let divideNum = Math.ceil(keysArray.length / len);

        if (i === num - 1) {
            divideNum = keysArray.length;
        }

        results.push(keysArray.splice(0, divideNum));
    }

    return results;
}
