import type { DataFrame } from '@terascope/data-mate';
import type { AnyObject, DataEntity, Logger } from '@terascope/utils';
import type {
    SearchParams
} from 'elasticsearch';
import type { EventEmitter } from 'events';
import type { DataTypeConfig, xLuceneVariables } from '@terascope/types';
import type { WindowState } from './WindowState';

/**
 * This is used for as the internal elasticsearch
 * client in reader, this is designed as an abstraction
 * so that spaces client will work with it is own specific
 * optimizations
*/
export interface ReaderClient {
    /**
     * Counts the number of documents for a given query
    */
    count(query: SearchParams): Promise<number>;

    /**
     * Searches for documents for a given query
    */
    search(
        query: SearchParams,
        responseType: FetchResponseType.raw,
        typeConfig?: DataTypeConfig
    ): Promise<Buffer>;
    search(
        query: SearchParams,
        responseType: FetchResponseType.data_entities,
        typeConfig?: DataTypeConfig
    ): Promise<DataEntity[]>;
    search(
        query: SearchParams,
        responseType: FetchResponseType.data_frame,
        typeConfig: DataTypeConfig
    ): Promise<DataFrame>;
    search(
        query: SearchParams,
        responseType: FetchResponseType,
        typeConfig?: DataTypeConfig
    ): Promise<DataEntity[]|DataFrame|Buffer>;

    /**
     * Used to make a custom search request
     *
     * @note this API is subject to change
    */
    _searchRequest(query: SearchParams, fullResponse?: false): Promise<DataEntity[]>;
    _searchRequest(query: SearchParams, fullResponse: true): Promise<unknown>;
    _searchRequest(query: SearchParams, fullResponse?: boolean): Promise<DataEntity[]|unknown>;

    /**
     * Gets the elasticsearch major server version,
     * this will be used to format the search parameters
    */
    getESVersion(): number;

    /**
     * Verify that the cluster is up,
     * internally this will use esClient.version() probably
    */
    verify(): Promise<void>;

    /**
     * Used to determine the max window size
    */
    getSettings(index: string): Promise<SettingResults>;
}

export interface SettingResults {
    [key: string]: {
        settings: {
            'index.max_result_window': number
        },
        defaults: AnyObject
    }
}

/**
 * An array of key spaces
*/
export interface IDSlicerRange {
    readonly keys: readonly string[];
    readonly count: number;
}
/**
 * This used a list of all of the ID slicer ranges, the
 * index of the range will correlate with the slicer instance
*/
export type IDSlicerRanges = readonly IDSlicerRange[];

export enum IDType {
    base64url = 'base64url',
    base64 = 'base64',
    hexadecimal = 'hexadecimal',
    HEXADECIMAL = 'HEXADECIMAL'
}

/**
 * This is used for an individual starting point
 * for a single range
*/
export interface DateSlicerRange {
    readonly dates: SlicerDates;
    readonly range: DateSegments;
    readonly interval: ParsedInterval;
    /**
     * This may be null sometimes
    */
    readonly count: number|null;
}

/**
 * This used a list of all of the Date slicer ranges, the
 * index of the range will correlate with the slicer instance
*/
export type DateSlicerRanges = readonly (DateSlicerRange|null)[];

export interface DateSegments {
    start: moment.Moment;
    limit: moment.Moment;
}

/**
 * This information is just used during the
 * slicing logic and not the fetch or count request
*/
export interface ReaderSliceMetadata {
    /**
     * The limit date of the whole slicer range
    */
    limit?: string;

    /**
     * This is just used as slice metadata,
    */
    holes?: readonly DateConfig[];
}

/**
 * This can be used to count or fetch and is the
 * metadata used stored on a slice
*/
export interface ReaderSlice extends ReaderSliceMetadata {
    /**
     * The query constraint provided in the config
    */
    query?: string;

    /**
     * The number of records to fetch,
     * for count set this will be set to 0,
     * for searching this will be set to the config size
    */
    count?: number;

    /**
     * The start of the date range
    */
    start?: string;

    /**
     * The end of the date range
    */
    end?: string;

    /**
     * Used for id wildcard queries,
     * each key is ORd together so one
     * query can be ran to the count for a single
     * slicer range
    */
    keys?: readonly string[];
}

export interface CountFn {
    (args: Pick<ReaderSlice, 'start'|'end'|'keys'>) : Promise<number>;
}

/**
 * Built from the config and {@link IDSlicerConfig}
 *
 * @internal
*/
export interface IDSlicerArgs {
    retryData?: string;
    logger: Logger;
    range?: ReaderSlice;
    keySet: readonly string[];
    baseKeyArray: readonly string[];
    events: EventEmitter;
    startingKeyDepth: number;
    countFn: CountFn;
    size: number;
}

export type RecoveryData = {
    slicer_id: number;
    lastSlice?: {
        /** A reserved key for sending work to a particular worker */
        request_worker?: string;
        /** The slice request can contain any metadata */
        [prop: string]: any;
    }
}

export interface IDSlicerConfig {
    slicerID: number,
    numOfSlicers: number,
    recoveryData?: RecoveryData[],
}

export interface DateConfig {
    start: string | moment.Moment;
    end: string | moment.Moment;
}

export interface SlicerArgs {
    timeResolution: moment.unitOfTime.Base;
    size: number;
    subsliceByKey?: boolean;
    subsliceKeyThreshold?: number;
    idFieldName?: string,
    keyType?: IDType;
    startingKeyDepth?: number,
    interval?: ParsedInterval;
    latencyInterval?: ParsedInterval;
    lifecycle: LifeCycle;
    numOfSlicers: number;
    logger: Logger;
    dates?: SlicerDates;
    /**
     * This only matters for persistent jobs
    */
    primaryRange?: DateSegments;
    /**
     * The slicer id, I think this is used to further subdivide
     * the dates
    */
    id: number;
    /**
     * This used to emit a slice recursion event
    */
    events: EventEmitter;
    /**
     * This is only used for persistent jobs
    */
    windowState?: WindowState;
    countFn: CountFn;
}

export interface SlicerDates extends DateSegments {
    end: moment.Moment;
    holes?: readonly DateConfig[];
}

/** What a date slicer fn will return */
export type DateSlicerResults = ReaderSlice | (readonly ReaderSlice[]) | null;
export type IDSlicerResults = ReaderSlice|null;

export type ParsedInterval = readonly [step: number, unit: moment.unitOfTime.Base];

export type DateSlicerMetadata = Record<number, {
    start: string;
    end: string;
} & GetIntervalResult>;
export type DateSlicerMetadataHook = (metadata: DateSlicerMetadata) => Promise<void>;

export type LifeCycle = 'once'|'persistent';

export interface DateSlicerArgs {
    lifecycle: LifeCycle,
    slicerID: number,
    numOfSlicers: number,
    recoveryData?: RecoveryData[];
    windowState?: WindowState,
    startTime?: Date | string
    hook?: DateSlicerMetadataHook;
}

export interface DateSlicerConfig {
    lifecycle: LifeCycle,
    slicerID: number,
    numOfSlicers: number,
    recoveryData?: RecoveryData[],
    windowState?: WindowState,
    startTime?: Date | string,
    hook?: DateSlicerMetadataHook;
}

export interface GetIntervalResult {
    /**
     * This is interval for the slicer, it will be null if
     * there is no data for this time period
    */
    readonly interval: ParsedInterval|null;
    /**
     * This is interval for the slicer, it will be null
     * when the interval in config is not set to auto
    */
    readonly count: number|null;
}

/**
 * This function is used to determine the interval for each slicer,
*/
export interface GetIntervalFn {
    (dates: DateSegments, slicerId: number): GetIntervalResult|Promise<GetIntervalResult>;
}

export interface StartPointConfig {
    dates: DateSegments;
    numOfSlicers: number;
    getInterval: GetIntervalFn;
    recoveryData?: ReaderSlice[];
}

export interface InputDateSegments {
    start: moment.Moment | string | Date;
    limit: moment.Moment | string | Date;
}

export enum FetchResponseType {
    data_entities = 'data_entities',
    data_frame = 'data_frame',
    raw = 'raw',
}

export interface ESReaderOptions {
    index: string;
    id_field_name?: string;
    size: number;
    start?: null | string;
    end?: null | string;
    interval: string;
    date_field_name: string;
    query?: string;
    fields?: null | string[];
    delay: string;
    subslice_by_key: boolean;
    subslice_key_threshold: number;
    key_type: IDType;
    key_range?: null | string[];
    time_resolution: moment.unitOfTime.Base;
    geo_field?: string;
    geo_box_top_left?: string;
    geo_box_bottom_right?: string;
    geo_point?: string;
    geo_distance?: string;
    geo_sort_point?: string;
    geo_sort_order?: string;
    geo_sort_unit?: string;
    connection: string;
    starting_key_depth: number;
    response_type?: FetchResponseType;
    type_config?: DataTypeConfig
    useSimpleFetch?: boolean;
}

export interface SpacesAPIConfig extends ESReaderOptions {
    endpoint: string;
    token: string;
    timeout: number;
    headers?: AnyObject;
    retry?: number;
    variables?: xLuceneVariables
}

export interface DetermineSliceResults {
    start: moment.Moment;
    end: moment.Moment;
    count: number;
    keys?: readonly string[];
}

export interface SlicerDateConfig extends DateSegments {
    end: moment.Moment;
    holes?: readonly DateConfig[];
}
