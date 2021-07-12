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
    dateOptions,
    processInterval,
    dateFormat,
    dateFormatSeconds,
    parseDate,
    delayedStreamSegment,
    determineIDSlicerRanges,
    determineDateSlicerRanges,
} from './algorithms';
import {
    ESReaderOptions,
    SlicerDateResults,
    DateSegments,
    InputDateSegments,
    SlicerArgs,
    IDType,
    DateSlicerArgs,
    IDSlicerArgs,
    IDSlicerConfig,
    DateSlicerResults,
    IDSlicerResults,
    ReaderClient,
    SettingResults,
    IDSlicerRanges,
    DateSlicerRanges,
    ParsedInterval,
    DateSlicerRange,
    IDSlicerRange
} from './interfaces';
import { WindowState } from './WindowState';
import { buildQuery } from './utils';

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
    ): Promise<ParsedInterval> {
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

    private validateIDSlicerConfig(config: unknown): void {
        if (isObject(config)) {
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
                    if (!areAllObjects) {
                        throw new Error('Input recoveryData must be an array of recovered slices, cannot have mixed values');
                    }
                } else {
                    throw new Error(`Input recoveryData must be an array of recovered slices, got ${getTypeOf(config.recoveryData)}`);
                }
            }
        } else {
            throw new Error(`Input must be an object, received ${getTypeOf(config)}`);
        }
    }

    /**
     * This used to subdivide all the of the key ranges for each
     * slicer instance, then each "range" should be passed into
     * {@link ElasticsearchReaderAPI.makeIDSlicerFromRange}
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
     * Returns an instance of the slicer using the id algorithm,
     * this is a higher level API and is not recommended when using many
     * slicers since making all of the slicer ranges at once is more efficient
    */
    async makeIDSlicer(config: IDSlicerConfig): Promise<() => Promise<IDSlicerResults>> {
        const ranges = this.makeIDSlicerRanges(config);
        return this.makeIDSlicerFromRange(config, ranges[config.slicerID]);
    }

    /**
     * Returns an instance of the slicer using the id algorithm,
     * from a given slicer range, this should be used in conjunction
     * with {@link ElasticsearchReaderAPI.makeIDSlicerRanges}
    */
    async makeIDSlicerFromRange(
        config: IDSlicerConfig,
        range: IDSlicerRange
    ): Promise<() => Promise<IDSlicerResults>> {
        this.validateIDSlicerConfig(config);

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
            keySet: range,
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

    private validateDateSlicerConfig(config: unknown): void {
        if (isObject(config)) {
            if (!(config.lifecycle === 'once' || config.lifecycle === 'persistent')) {
                throw new Error('Parameter lifecycle must be set to "once" or "persistent"');
            }

            if (!isNumber(config.numOfSlicers)) {
                throw new Error(`Parameter numOfSlicers must be a number, got ${getTypeOf(config.numOfSlicers)}`);
            }

            if (config.recoveryData) {
                if (Array.isArray(config.recoveryData)) {
                    const areAllObjects = config.recoveryData.every(isSimpleObject);
                    if (!areAllObjects) {
                        throw new Error('Input recoveryData must be an array of recovered slices, cannot have mixed values');
                    }
                } else {
                    throw new Error(`Input recoveryData must be an array of recovered slices, got ${getTypeOf(config.recoveryData)}`);
                }
            }

            if (config.hook && !isFunction(config.hook)) {
                throw new Error('Input hook must be a function if provided');
            }
        } else {
            throw new Error(`Input must be an object, received ${getTypeOf(config)}`);
        }
    }

    /**
     * This used to subdivide all the of the date ranges for each
     * slicer instance, then each "range" should be passed into
     * {@link ElasticsearchReaderAPI.makeDateSlicerFromRange}
    */
    async makeDateSlicerRanges(config: Omit<DateSlicerArgs, 'slicerID'|'windowState'>): Promise<DateSlicerRanges|undefined> {
        this.validateDateSlicerConfig(config);
        const {
            lifecycle,
            numOfSlicers,
        } = config;

        const isPersistent = lifecycle === 'persistent';

        await this.verifyIndex();

        const recoveryData = config.recoveryData?.map(
            (slice) => slice.lastSlice
        ).filter(Boolean) as SlicerDateResults[]|undefined || [];

        if (isPersistent) {
            // we need to interval to get starting dates
            const [interval, latencyInterval] = await Promise.all([
                this.determineSliceInterval(this.config.interval),
                this.determineSliceInterval(this.config.delay)
            ]);

            const { start, limit } = delayedStreamSegment(
                config.startTime,
                interval,
                latencyInterval
            );

            return determineDateSlicerRanges({
                dates: { start, limit },
                numOfSlicers,
                recoveryData,
                getInterval: async (dates) => this.determineSliceInterval(
                    this.config.interval,
                    dates,
                )
            });
        }

        const _esDates = await this.determineDateRanges();
        // query with no results
        if (_esDates.start == null || _esDates.limit == null) {
            this.logger.warn(`No data was found in index: ${this.config.index} using query: ${this.config.query}`);
            // slicer will run and complete when a null is returned
            return;
        }
        const dates = _esDates as DateSegments;

        return determineDateSlicerRanges({
            dates,
            numOfSlicers,
            recoveryData,
            getInterval: async () => {
                const interval = await this.determineSliceInterval(
                    this.config.interval,
                    dates
                );
                // This was originally created to update the job configuration
                // with the correct interval so that retries and recovery operates
                // with more accuracy. Also it exposes the discovered interval to
                // to the user
                if (config.hook) {
                    await config.hook({
                        interval,
                        start: moment(dates.start.format(this.dateFormat)).toISOString(),
                        end: moment(dates.limit.format(this.dateFormat)).toISOString(),
                    });
                }
                return interval;
            }
        });
    }

    /**
     * Returns an instance of the slicer using the date algorithm,
     * this is a higher level API and is not recommended when using many
     * slicers since making all of the slicer ranges at once is more efficient
    */
    async makeDateSlicer(config: DateSlicerArgs): Promise<() => Promise<DateSlicerResults>> {
        const ranges = await this.makeDateSlicerRanges(config);
        if (ranges == null || ranges[config.slicerID] == null) {
            // if it gets here there is probably no data for the query
            return async () => null;
        }
        return this.makeDateSlicerFromRange(config, ranges[config.slicerID]);
    }

    /**
     * Returns an instance of the slicer using the date algorithm,
     * from a given slicer range, this should be used in conjunction
     * with {@link ElasticsearchReaderAPI.makeDateSlicerRanges}
    */
    async makeDateSlicerFromRange(
        config: Omit<DateSlicerArgs, 'recoveryData'>, range: DateSlicerRange
    ): Promise<() => Promise<DateSlicerResults>> {
        if (!isNumber(config.slicerID)) {
            throw new Error(`Parameter slicerID must be a number, got ${getTypeOf(config.slicerID)}`);
        }
        this.validateDateSlicerConfig(config);

        const {
            slicerID,
            lifecycle,
            numOfSlicers,
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

        if (isPersistent) {
            const windowState = config.windowState as WindowState|undefined;
            if (!windowState || !windowState.checkin) {
                throw new Error(`Invalid parameter windowState, must provide a valid windowState in "persistent" mode, got ${getTypeOf(windowState)}`);
            }
            if (!config.startTime || !isValidDate(config.startTime)) {
                throw new Error(`Invalid parameter startTime, must provide a valid date in "persistent" mode, got ${getTypeOf(config.startTime)}`);
            }

            // we need to interval to get starting dates
            const [interval, latencyInterval] = await Promise.all([
                this.determineSliceInterval(this.config.interval),
                this.determineSliceInterval(this.config.delay)
            ]);

            return dateSlicer({
                ...slicerFnArgs,
                interval,
                latencyInterval,
                windowState,
                dates: range.dates,
                primaryRange: range.range,
            });
        }

        const esDates = await this.determineDateRanges();
        // query with no results
        if (esDates.start == null || esDates.limit == null) {
            this.logger.warn(`No data was found in index: ${this.config.index} using query: ${this.config.query}`);
            // slicer will run and complete when a null is returned
            return async () => null;
        }

        // we do not care for range for once jobs
        return dateSlicer({
            ...slicerFnArgs,
            interval: range.interval,
            dates: range.dates,
        });
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
