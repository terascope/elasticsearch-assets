import { EventEmitter } from 'events';
import {
    AnyObject,
    DataEntity,
    isObjectEntity,
    getTypeOf,
    Logger,
    toNumber,
    isSimpleObject,
    isNumber,
    isValidDate,
    isFunction,
    isString,
    isWildCardString,
    matchWildcard
} from '@terascope/utils';
import { DataFrame } from '@terascope/data-mate';
import { DataTypeConfig } from '@terascope/data-types';
import moment from 'moment';
import type { CountParams, SearchParams } from 'elasticsearch';
import {
    dateSlicer,
    idSlicer,
    getKeyArray,
    buildQuery,
    dateOptions,
    processInterval,
    dateFormat,
    dateFormatSeconds,
    parseDate,
    determineStartingPoint,
    delayedStreamSegment,
    determineIDSlicerRanges
} from './algorithms';
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
    IDSlicerConfig,
    DateSlicerResults,
    IDSlicerResults,
    ReaderClient,
    SettingResults,
    IDSlicerRanges
} from './interfaces';
import { WindowState } from './WindowState';

type FetchDate = moment.Moment | null;

function isValidDataTypeConfig(record: unknown): record is DataTypeConfig {
    if (!record || !isSimpleObject(record)) return false;
    if (!record.version || !isNumber(record.version)) return false;
    if (!record.fields || !isSimpleObject(record.fields)) return false;

    return true;
}

export class ElasticsearchReaderAPI {
    readonly config: ESReaderOptions;
    logger: Logger;
    protected readonly client: ReaderClient;
    private windowSize: number|undefined = undefined;
    protected readonly dateFormat: string;
    protected readonly emitter: EventEmitter;

    constructor(
        config: ESReaderOptions, client: ReaderClient, emitter: EventEmitter, logger: Logger
    ) {
        const { time_resolution } = config;

        if (config.use_data_frames) {
            if (!isValidDataTypeConfig(config.type_config)) {
                throw new Error('Parameter "type_config" must be set if DataFrames are being returned');
            }
        }

        this.config = Object.freeze(config);
        this.emitter = emitter;
        this.logger = logger;
        this.client = client;
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
        if (!this.windowSize) await this.setWindowSize();

        // if we did go ahead and complete query
        const query = buildQuery(this.config, queryParams);
        query.size = this.windowSize;

        return this.client.search(
            query, this.config.use_data_frames ?? false, this.config.type_config
        );
    }

    _searchRequest(query: SearchParams, fullResponse?: false): Promise<DataEntity[]>;
    _searchRequest(query: SearchParams, fullResponse: true): Promise<unknown>;
    async _searchRequest(
        query: SearchParams, fullResponse?: boolean
    ): Promise<DataEntity[]|unknown> {
        return this.client._searchRequest(
            query,
            fullResponse
        );
    }

    async determineSliceInterval(
        interval: string, esDates?: InputDateSegments
    ): Promise<[number, moment.unitOfTime.Base]> {
        if (this.config.interval !== 'auto') {
            return processInterval(interval, this.config.time_resolution, esDates);
        }

        if (esDates == null) {
            throw new Error('Missing required dates to create interval');
        }

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

        const millisecondIntervalResults = millisecondInterval < 1 ? 1 : millisecondInterval;
        return [millisecondIntervalResults, 'ms'];
    }

    async setWindowSize(): Promise<void> {
        const { size } = this.config;
        const windowSize = await this.getWindowSize();
        if (size > windowSize) {
            throw new Error(`Invalid parameter size: ${size}, it cannot exceed the "index.max_result_window" index setting of ${windowSize} for index ${this.config.index}`);
        }
        this.windowSize = windowSize;
    }

    /**
     * Use this determine all of the slice ranges for all of the slicers
    */
    makeIDSlicerRanges(config: Pick<IDSlicerConfig, 'keyRange'|'keyType'|'numOfSlicers'>): IDSlicerRanges {
        const {
            numOfSlicers,
            keyRange,
            keyType,
        } = config;

        if (!keyType || !(keyType in IDType)) {
            throw new Error(`Invalid parameter key_type, got ${keyType}`);
        }

        if (keyRange) {
            if (keyRange.length === 0) {
                throw new Error('Invalid key_range parameter, must be an array with length > 0');
            }
            if (!keyRange.every(isString)) {
                throw new Error('Invalid key_range parameter, must be an array of strings');
            }
        }

        if (!isNumber(numOfSlicers)) {
            throw new Error(`Parameter numOfSlicers must be a number, got ${getTypeOf(numOfSlicers)}`);
        }

        const baseKeyArray = getKeyArray(keyType);
        // we slice as not to mutate for when this is called again
        const keyArray: readonly string[] = keyRange ? keyRange.slice() : baseKeyArray;

        if (difference(keyArray, baseKeyArray).length > 0) {
            throw new Error(`key_range specified for id_reader contains keys not found in: ${keyType}`);
        }

        return determineIDSlicerRanges(keyArray, numOfSlicers);
    }

    /**
     * Return an instance of the slicer using the id algorithm
    */
    async makeIDSlicer(config: IDSlicerConfig): Promise<() => Promise<IDSlicerResults>> {
        if (!isNumber(config.slicerID)) {
            throw new Error(`Parameter slicerID must be a number, got ${getTypeOf(config.slicerID)}`);
        }

        if (this.version >= 6 && (
            !isString(config.idFieldName) || config.idFieldName.length === 0
        )) {
            throw new Error(`Parameter idFieldName must be a string, got ${getTypeOf(config.idFieldName)}`);
        }

        if (config.recoveryData) {
            if (Array.isArray(config.recoveryData)) {
                const areAllObjects = config.recoveryData.every(isSimpleObject);
                if (!areAllObjects) throw new Error('Input recoveryData must be an array of recovered slices, cannot have mixed values');
            } else {
                throw new Error(`Input recoveryData must be an array of recovered slices, got ${getTypeOf(config.recoveryData)}`);
            }
        }

        const ranges = this.makeIDSlicerRanges(config);

        const countFn = this.count.bind(this);

        const {
            slicerID,
            keyRange,
            keyType,
            recoveryData,
            startingKeyDepth,
            idFieldName,
        } = config;
        const { type, size } = this.config;

        const baseKeyArray = getKeyArray(keyType);
        // we slice as not to mutate for when this is called again
        const keyArray: readonly string[] = keyRange ? keyRange.slice() : baseKeyArray;

        if (difference(keyArray, baseKeyArray).length > 0) {
            throw new Error(`key_range specified for id_reader contains keys not found in: ${keyType}`);
        }

        if (!this.windowSize) await this.setWindowSize();

        const slicerConfig: IDSlicerArgs = {
            events: this.emitter,
            logger: this.logger,
            keySet: ranges[slicerID],
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
            const parsedRetry = recoveryData.map((obj) => {
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

        return idSlicer(slicerConfig);
    }

    private validateDateSlicerConfig(input: unknown): DateSlicerConfig {
        if (isObject(input)) {
            if (!(input.lifecycle === 'once' || input.lifecycle === 'persistent')) {
                throw new Error('Parameter lifecycle must be set to "once" or "persistent"');
            }
            if (!isNumber(input.slicerID)) {
                throw new Error(`Parameter slicerID must be a number, got ${getTypeOf(input.slicerID)}`);
            }
            if (!isNumber(input.numOfSlicers)) {
                throw new Error(`Parameter numOfSlicers must be a number, got ${getTypeOf(input.numOfSlicers)}`);
            }

            if (input.recoveryData) {
                if (Array.isArray(input.recoveryData)) {
                    const areAllObjects = input.recoveryData.every(isSimpleObject);
                    if (!areAllObjects) {
                        throw new Error('Input recoveryData must be an array of recovered slices, cannot have mixed values');
                    }
                } else {
                    throw new Error(`Input recoveryData must be an array of recovered slices, got ${getTypeOf(input.recoveryData)}`);
                }
            } else {
                input.recoveryData = [];
            }

            if (input.lifecycle === 'persistent') {
                const windowState = input.windowState as WindowState|undefined;
                if (!windowState || !windowState.checkin) {
                    throw new Error(`Invalid parameter windowState, must provide a valid windowState in "persistent" mode, got ${getTypeOf(input.windowState)}`);
                }
                if (!input.startTime || !isValidDate(input.startTime)) {
                    throw new Error(`Invalid parameter startTime, must provide a valid date in "persistent" mode, got ${getTypeOf(input.startTime)}`);
                }
            }

            if (input.hook && !isFunction(input.hook)) {
                throw new Error('Input hook must be a function if provided');
            }
        } else {
            throw new Error(`Input must be an object, received ${getTypeOf(input)}`);
        }

        return input as unknown as DateSlicerConfig;
    }

    /**
     * Return an instance of the slicer using the date algorithm
    */
    async makeDateSlicer(args: DateSlicerArgs): Promise<() => Promise<DateSlicerResults>> {
        const config = this.validateDateSlicerConfig(args);
        const {
            slicerID,
            lifecycle,
            numOfSlicers,
            windowState,
        } = config;

        const isPersistent = lifecycle === 'persistent';
        const countFn = this.count.bind(this);

        const {
            time_resolution: timeResolution,
            size,
            subslice_by_key: subsliceByKey,
            subslice_key_threshold: subsliceKeyThreshold,
            key_type: keyType,
            id_field_name: idFieldName,
            starting_key_depth: startingKeyDepth,
            type
        } = this.config;

        if (!this.windowSize) await this.setWindowSize();

        const slicerFnArgs: SlicerArgs = {
            lifecycle,
            numOfSlicers,
            logger: this.logger,
            id: slicerID,
            events: this.emitter,
            version: this.version,
            countFn,
            windowState,
            timeResolution,
            size,
            subsliceByKey,
            subsliceKeyThreshold,
            keyType,
            idFieldName,
            startingKeyDepth,
            type
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
                config.startTime,
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
            const { dates, range } = determineStartingPoint(startConfig);

            slicerFnArgs.dates = dates;
            slicerFnArgs.primaryRange = range;
            return dateSlicer(slicerFnArgs);
        }

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
        const { dates } = determineStartingPoint(startConfig);
        slicerFnArgs.dates = dates;
        return dateSlicer(slicerFnArgs);
    }

    async determineDateRanges(): Promise<{ start: FetchDate; limit: FetchDate; }> {
        const [start, limit] = await Promise.all([
            this.getIndexDate(this.config.start, 'start'),
            this.getIndexDate(this.config.end, 'end')
        ]);
        return { start, limit };
    }

    private async getIndexDate(date: string|null|undefined, order: string): Promise<FetchDate> {
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
        const [data] = await this._searchRequest(query, false);

        if (data == null) {
            this.logger.warn(`no data was found using query ${JSON.stringify(query)} for index: ${this.config.index}`);
            return null;
        }

        if (data[this.config.date_field_name] == null) {
            throw new Error(`Invalid date_field_name: "${this.config.date_field_name}" for index: ${this.config.index}, field does not exist on record`);
        }

        if (order === 'start') {
            return parseDate(data[this.config.date_field_name]);
        }
        // end date is non-inclusive, adding 1s so range will cover it
        const newDate = data[this.config.date_field_name];
        const time = moment(newDate).add(1, this.config.time_resolution);
        return parseDate(time.format(this.dateFormat));
    }

    /**
     * Get the index settings, used to determine the max_result_window size
    */
    async getSettings(index: string): Promise<SettingResults> {
        return this.client.getSettings(index);
    }

    /**
     * This used verify the index.max_result_window size
     * will be big enough to fix the within the requested
     * slice size
    */
    async getWindowSize(): Promise<number> {
        const window = 'index.max_result_window';
        const { index } = this.config;

        const settings = await this.getSettings(index);
        const matcher = indexMatcher(index);

        for (const [key, configs] of Object.entries(settings)) {
            if (matcher(key)) {
                const defaultPath = configs.defaults[window];
                const configPath = configs.settings[window];
                // config goes first as it overrides an defaults
                if (configPath) return toNumber(configPath);
                if (defaultPath) return toNumber(defaultPath);
            }
        }

        return this.config.size;
    }

    get version(): number {
        return this.client.getESVersion();
    }

    async verifyIndex(): Promise<void> {
        // this is method in api is badly named
        return this.client.verify();
    }
}

function isObject(val: unknown): val is AnyObject {
    return isObjectEntity(val);
}

function difference(
    srcArray: readonly string[] | null,
    valArray: readonly string[]
): readonly string[] {
    const results: string[] = [];
    if (!srcArray) return results;

    for (const val of srcArray) {
        if (!valArray.includes(val)) {
            results.push(val);
        }
    }
    return results;
}

function indexMatcher(index: string): (input: string) => boolean {
    if (isWildCardString(index)) {
        return (indexVal) => matchWildcard(index, indexVal);
    }

    return (indexVal) => indexVal.includes(index);
}
