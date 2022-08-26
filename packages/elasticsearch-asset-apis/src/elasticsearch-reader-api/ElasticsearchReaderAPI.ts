import { EventEmitter } from 'events';
import {
    AnyObject,
    DataEntity,
    isObjectEntity,
    getTypeOf,
    Logger,
    isSimpleObject,
    isNumber,
    isValidDate,
    isFunction,
    isString,
    isWildCardString,
    matchWildcard,
    pRetry,
    toIntegerOrThrow
} from '@terascope/utils';
import { DataFrame } from '@terascope/data-mate';
import { DataTypeConfig } from '@terascope/data-types';
import moment from 'moment';
import type { CountParams, SearchParams } from 'elasticsearch';
import { inspect } from 'util';
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
    DateSegments,
    InputDateSegments,
    SlicerArgs,
    IDType,
    DateSlicerArgs,
    IDSlicerArgs,
    IDSlicerConfig,
    DateSlicerResults,
    ReaderClient,
    SettingResults,
    IDSlicerRanges,
    DateSlicerRanges,
    DateSlicerRange,
    IDSlicerRange,
    DateSlicerMetadata,
    GetIntervalResult,
    ReaderSlice,
    IDSlicerResults,
    FetchResponseType
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
    /**
     * we should expose this because in some cases
     * it might be an optimization to set this externally
    */
    windowSize: number|undefined = undefined;
    protected readonly dateFormat: string;
    protected readonly emitter: EventEmitter;

    constructor(
        config: ESReaderOptions, client: ReaderClient, emitter: EventEmitter, logger: Logger
    ) {
        const { time_resolution } = config;

        if (config.response_type === FetchResponseType.data_frame) {
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
        this.count = this.count.bind(this);
    }

    makeWindowState(numOfSlicers: number): WindowState {
        return new WindowState(numOfSlicers);
    }

    async count(queryParams: ReaderSlice = {}): Promise<number> {
        const query = buildQuery(this.config, { ...queryParams, count: 0 });
        return this.client.count(query as CountParams);
    }

    /**
     * Fetch a given slice, the data will be returned in the format
     * specified under `response_type` in the config
     *
     * `fetch` now sets the query `size` parameter to 1.5 times the count
     * recorded in the slice, rather than the window size of the index.
     * It will retry up to five times if the size of the result set comes back
     * the same size as the querySize, this is meant to ensure all of the
     * records are retrieved, but should be unlikely to happen in most cases
     *
     * When it previously used the indices max window size, it appeared to
     * slow ES queries down significantly, refs:
     *   https://github.com/terascope/elasticsearch-assets/issues/948
     */
    async fetch(queryParams: ReaderSlice = {}): Promise<DataEntity[]|DataFrame|Buffer> {
        const countExpansionFactor = 1.5;
        let querySize = 10000;
        const retryLimit = 5;

        // attempt to get window if not set (sets this.windowSize as side effect)
        if (!this.windowSize) await this.setWindowSize();

        // set querySize to the lesser of expandedSize or windowSize
        if (queryParams.count) {
            const expandedSize = Math.ceil(queryParams.count * countExpansionFactor);
            if (this.windowSize) {
                if (expandedSize >= this.windowSize) {
                    throw new Error(`The query size, ${expandedSize}, is greater than the index.max_result_window: ${this.windowSize}`);
                } else {
                    querySize = expandedSize;
                }
            }
        }

        const _fetch = async ():
        Promise<DataEntity[]|DataFrame|Buffer> => {
            const query = buildQuery(this.config, {
                ...queryParams, count: querySize
            });

            const result = await this.client.search(
                query,
                this.config.response_type ?? FetchResponseType.data_entities,
                this.config.type_config
            );

            const resultSize = this._getResultSize(result);

            if (resultSize === querySize) {
                // it's unlikely that this condition would be true without
                // resultSize actually being larger than querySize so we will
                // throw away these results, expand querySize and query again
                // by relying on pRetry
                const expandedSize = Math.ceil(querySize * countExpansionFactor);
                if (this.windowSize) {
                    if (expandedSize >= this.windowSize) {
                        throw new Error(`The query size, ${expandedSize}, is greater than the index.max_result_window: ${this.windowSize}`);
                    } else {
                        querySize = expandedSize;
                    }
                }
                const msg = `The result set contained exactly ${resultSize} records, searching again with size: ${expandedSize}`;
                this.logger.debug(msg);
                throw new Error(msg); // throw for pRetry
            }

            return result;
        };

        const result = await pRetry(() => _fetch(),
            {
                backoff: 1.1,
                delay: 250,
                retries: retryLimit,
                matches: ['result set contained exactly']
                // reason disable due to this bug:
                // https://github.com/terascope/teraslice/issues/3286
                // reason: `Retry limit (${retryLimit}) hit`
            }
        );
        return result;
    }

    /**
     * Handles multiple result types and returns the number of returned records
     * @param result the object returned that contains the search results
     * @returns the number of records returned by the search
     */
    _getResultSize(result: DataEntity[]|DataFrame|Buffer): number {
        let resultSize;
        if (Buffer.isBuffer(result)) {
            const json = result.toJSON();
            resultSize = json.data.length;
        } else if (DataEntity.isDataEntityArray(result)) {
            resultSize = result.length;
        } else {
            resultSize = result.toArray().length;
        }
        return resultSize;
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
    ): Promise<GetIntervalResult> {
        if (this.config.interval !== 'auto') {
            return {
                interval: processInterval(interval, this.config.time_resolution, esDates),
                count: null,
            };
        }

        if (esDates == null) {
            throw new Error('Missing required dates to create interval');
        }

        const count = await this.count({
            start: moment(esDates.start).format(this.dateFormat),
            end: moment(esDates.limit).format(this.dateFormat),
        });

        // we need to return early so the millisecondInterval doesn't
        // end up being Infinity because 1/0 === Infinity
        if (count === 0) {
            return {
                interval: null,
                count
            };
        }

        const numOfSlices = Math.ceil(count / this.config.size);
        const timeRangeMilliseconds = moment(esDates.limit).diff(esDates.start);
        const millisecondInterval = Math.floor(timeRangeMilliseconds / numOfSlices);

        if (this.config.time_resolution === 's') {
            let seconds = Math.floor(millisecondInterval / 1000);
            if (seconds < 1) seconds = 1;
            if (!Number.isSafeInteger(seconds)) {
                throw new Error(`Invalid interval diff found "${inspect(seconds)}" ${inspect({
                    esDates,
                    numOfSlices,
                    count,
                    millisecondInterval,
                    seconds,
                    config: this.config
                })}`);
            }
            return { interval: [seconds, 's'], count };
        }

        const millisecondIntervalResults = millisecondInterval < 1 ? 1 : millisecondInterval;
        if (!Number.isSafeInteger(millisecondIntervalResults)) {
            throw new Error(`Invalid interval diff found "${inspect(millisecondIntervalResults)}" ${inspect({
                esDates,
                millisecondInterval,
                config: this.config
            })}`);
        }
        return {
            interval: [millisecondIntervalResults, 'ms'],
            count
        };
    }

    async setWindowSize(): Promise<void> {
        const { size } = this.config;
        const windowSize = await this.getWindowSize();
        if (size > windowSize) {
            throw new Error(`Invalid parameter size: ${size}, it cannot exceed the "index.max_result_window" index setting of ${windowSize} for index ${this.config.index}`);
        }
        this.windowSize = windowSize;
    }

    private validateIDSlicerConfig(config: IDSlicerConfig): void {
        if (isObject(config)) {
            if (!isNumber(config.slicerID)) {
                throw new Error(`Parameter slicerID must be a number, got ${getTypeOf(config.slicerID)}`);
            }

            if (
                !isString(this.config.id_field_name) || this.config.id_field_name.length === 0
            ) {
                throw new Error(`Parameter idFieldName must be a string, got ${getTypeOf(this.config.id_field_name)}`);
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
    async makeIDSlicerRanges(
        config: Pick<IDSlicerConfig, 'numOfSlicers'>
    ): Promise<IDSlicerRanges> {
        const {
            numOfSlicers,
        } = config;

        if (!this.config.key_type || !(this.config.key_type in IDType)) {
            throw new Error(`Invalid parameter key_type, got ${this.config.key_type}`);
        }

        if (this.config.key_range) {
            if (this.config.key_range.length === 0) {
                throw new Error('Invalid key_range parameter, must be an array with length > 0');
            }
            if (!this.config.key_range.every(isString)) {
                throw new Error('Invalid key_range parameter, must be an array of strings');
            }
        }

        if (!isNumber(numOfSlicers)) {
            throw new Error(`Parameter numOfSlicers must be a number, got ${getTypeOf(numOfSlicers)}`);
        }

        const baseKeyArray = getKeyArray(this.config.key_type);
        // we slice as not to mutate for when this is called again
        const keyArray: readonly string[] = this.config.key_range
            ? this.config.key_range.slice()
            : baseKeyArray;

        if (difference(keyArray, baseKeyArray).length > 0) {
            throw new Error(`key_range specified for id_reader contains keys not found in: ${this.config.key_type}`);
        }

        return determineIDSlicerRanges(
            keyArray, numOfSlicers, this.count
        );
    }

    /**
     * Returns an instance of the slicer using the id algorithm,
     * this is a higher level API and is not recommended when using many
     * slicers since making all of the slicer ranges at once is more efficient
    */
    async makeIDSlicer(config: IDSlicerConfig): Promise<() => Promise<IDSlicerResults>> {
        const ranges = await this.makeIDSlicerRanges(config);
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

        const {
            slicerID,
            recoveryData,
        } = config;
        const { size } = this.config;

        const baseKeyArray = getKeyArray(this.config.key_type);
        // we slice as not to mutate for when this is called again
        const keyArray: readonly string[] = this.config.key_range
            ? this.config.key_range.slice()
            : baseKeyArray;

        if (difference(keyArray, baseKeyArray).length > 0) {
            throw new Error(`key_range specified for id_reader contains keys not found in: ${this.config.key_type}`);
        }

        if (!this.windowSize) await this.setWindowSize();

        const slicerConfig: IDSlicerArgs = {
            events: this.emitter,
            logger: this.logger,
            keySet: range.keys.slice(),
            baseKeyArray,
            startingKeyDepth: this.config.starting_key_depth,
            countFn: this.count,
            size
        };

        if (recoveryData && recoveryData.length > 0) {
            // TODO: verify what retryData is
            // real retry of executionContext here, need to reformat retry data
            const parsedRetry: (string|undefined)[] = recoveryData.map((obj) => {
                const slice = (obj.lastSlice as ReaderSlice|undefined);
                // when we get here there should only be one key
                if (slice?.keys?.length === 1) {
                    return slice.keys[0];
                }
                // some slicers might not have a previous slice, need to start from scratch
                return undefined;
            });

            slicerConfig.retryData = parsedRetry[slicerID];
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
        ).filter(Boolean) as ReaderSlice[]|undefined || [];

        if (isPersistent) {
            // we need to interval to get starting dates
            const [{ interval }, { interval: latencyInterval }] = await Promise.all([
                this.determineSliceInterval(this.config.interval),
                this.determineSliceInterval(this.config.delay)
            ]);

            if (interval == null || latencyInterval == null) {
                this.logger.warn(`No data was found in index: ${this.config.index} using query: ${this.config.query}`);
                // slicer will run and complete when a null is returned
                return;
            }

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

        const allSlicerDates = _esDates as DateSegments;
        const slicerMetadata: DateSlicerMetadata = {};

        const slicerRanges = await determineDateSlicerRanges({
            dates: allSlicerDates,
            numOfSlicers,
            recoveryData,
            getInterval: async (dates, slicerId) => {
                const result = await this.determineSliceInterval(
                    this.config.interval,
                    dates
                );

                slicerMetadata[slicerId] = {
                    ...result,
                    start: moment(dates.start.format(this.dateFormat)).toISOString(),
                    end: moment(dates.limit.format(this.dateFormat)).toISOString(),
                };

                if (result.interval == null) {
                    this.logger.warn(dates, `No data was found in index: ${this.config.index} using query: ${this.config.query} for slicer range`);
                    return result;
                }

                return result;
            }
        });

        // This was originally created to update the job configuration
        // with the correct interval so it exposes the discovered intervals
        // and date ranges for each slicer to the user
        if (config.hook) {
            await config.hook(slicerMetadata);
        }
        return slicerRanges;
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
            // the logging is done elsewhere
            return async () => null;
        }
        return this.makeDateSlicerFromRange(config, ranges[config.slicerID]!);
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
        const {
            time_resolution: timeResolution,
            size,
            subslice_by_key: subsliceByKey,
            subslice_key_threshold: subsliceKeyThreshold,
            key_type: keyType,
            id_field_name: idFieldName,
            starting_key_depth: startingKeyDepth
        } = this.config;

        if (!this.windowSize) await this.setWindowSize();

        const slicerFnArgs: SlicerArgs = {
            lifecycle,
            numOfSlicers,
            logger: this.logger,
            id: slicerID,
            events: this.emitter,
            countFn: this.count,
            timeResolution,
            size,
            subsliceByKey,
            subsliceKeyThreshold,
            keyType,
            idFieldName,
            startingKeyDepth
        };

        if (isPersistent) {
            const windowState = config.windowState as WindowState|undefined;
            if (!windowState || !windowState.checkin) {
                throw new Error(`Invalid parameter windowState, must provide a valid windowState in "persistent" mode, got ${getTypeOf(windowState)}`);
            }
            if (!config.startTime || !isValidDate(config.startTime)) {
                throw new Error(`Invalid parameter startTime, must provide a valid date in "persistent" mode, got ${getTypeOf(config.startTime)}`);
            }

            // we need to interval to get starting dates
            const [{ interval }, { interval: latencyInterval }] = await Promise.all([
                this.determineSliceInterval(this.config.interval),
                this.determineSliceInterval(this.config.delay)
            ]);

            if (interval == null || latencyInterval == null) {
                this.logger.warn(`No data was found in index: ${this.config.index} using query: ${this.config.query}`);
                // slicer will run and complete when a null is returned
                return async () => null;
            }

            return dateSlicer({
                ...slicerFnArgs,
                interval,
                latencyInterval,
                windowState,
                dates: range.dates,
                primaryRange: range.range,
            });
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
            this.logger.warn(`No data was found using query ${JSON.stringify(query)} for index: ${this.config.index}`);
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
                if (configPath) return toIntegerOrThrow(configPath);
                if (defaultPath) return toIntegerOrThrow(defaultPath);
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
