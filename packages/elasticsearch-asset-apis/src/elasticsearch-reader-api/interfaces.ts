import type { DataFrame } from '@terascope/data-mate';
import type { AnyObject, DataEntity, Logger } from '@terascope/utils';
import type {
    SearchParams
} from 'elasticsearch';
import type { LifeCycle, SlicerRecoveryData } from '@terascope/job-components';
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
        useDataFrames: false,
        typeConfig?: DataTypeConfig
    ): Promise<DataEntity[]>;
    search(
        query: SearchParams,
        useDataFrames: true,
        typeConfig: DataTypeConfig
    ): Promise<DataFrame>;
    search(
        query: SearchParams,
        useDataFrames: boolean,
        typeConfig?: DataTypeConfig
    ): Promise<DataEntity[]|DataFrame>;

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
export type IDSlicerRange = readonly string[];
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

export interface WildCardQuery {
    field: string;
    value: string;
}

export interface IDSlicerResult {
    count: number;
    wildcard: WildCardQuery;
}

/**
 * This is used for an individual starting point
 * for a single range
*/
export interface DateSlicerRange {
    readonly dates: SlicerDates;
    readonly range: DateSegments;
    readonly interval: ParsedInterval;
}

/**
 * This used a list of all of the Date slicer ranges, the
 * index of the range will correlate with the slicer instance
*/
export type DateSlicerRanges = readonly DateSlicerRange[];

export interface DateSegments {
    start: moment.Moment;
    limit: moment.Moment;
}

export interface IDSlicerArgs {
    retryData?: any;
    logger: Logger;
    range?: SlicerDateResults;
    keySet: readonly string[];
    baseKeyArray: readonly string[];
    events: EventEmitter;
    startingKeyDepth: number;
    version: number;
    countFn: (args: AnyObject) => Promise<number>;
    type?: string | null;
    idFieldName?: string | null;
    size: number;
}

export interface IDSlicerConfig {
    slicerID: number,
    numOfSlicers: number,
    recoveryData: SlicerRecoveryData[],
    keyType: IDType;
    keyRange?: string[];
    startingKeyDepth: number,
    idFieldName: string | null;
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
    type?: string | null;
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
    /**
     * The elasticsearch server version (only the major version)
    */
    version: number;
    countFn: (args: AnyObject) => Promise<number>
}
export interface SlicerDateResults {
    start: string;
    end: string;
    limit: string;
    count: number;
    holes?: DateConfig[];
    wildcard?: WildCardQuery;
    key?: string;
}

export interface SlicerDates extends DateSegments {
    end: moment.Moment;
    holes?: DateConfig[];
}

export interface IDReaderSlice {
    start?: string;
    end?: string;
    count: number;
    holes?: DateConfig[];
    wildcard?: WildCardQuery;
    key?: string;
}

/** What a date slicer fn will return */
export type DateSlicerResults = SlicerDateResults | SlicerDateResults[] | null;

/** What a id slicer fn will return */
export type IDSlicerResults = IDReaderSlice | null;

export type ParsedInterval = readonly [step: number, unit: moment.unitOfTime.Base];

export interface DateSlicerArgs {
    lifecycle: LifeCycle,
    slicerID: number,
    numOfSlicers: number,
    recoveryData?: SlicerRecoveryData[];
    windowState?: WindowState,
    startTime?: Date | string
    hook?: (args: {
        interval: ParsedInterval,
        start: string;
        end: string;
    }) => Promise<void>
}

export interface DateSlicerConfig {
    lifecycle: LifeCycle,
    slicerID: number,
    numOfSlicers: number,
    recoveryData: SlicerRecoveryData[],
    windowState?: WindowState,
    startTime?: Date | string,
    hook?: (args: {
        interval: ParsedInterval,
        start: string;
        end: string;
    }) => Promise<void>
}

export interface StartPointConfig {
    dates: DateSegments;
    numOfSlicers: number;
    getInterval: (dates: DateSegments) => ParsedInterval|Promise<ParsedInterval>;
    recoveryData?: SlicerDateResults[];
}

export interface InputDateSegments {
    start: moment.Moment | string | Date;
    limit: moment.Moment | string | Date;
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
    type?: string | null;
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
    use_data_frames?: boolean;
    type_config?: DataTypeConfig
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
    key?: string;
}

export interface SlicerDateConfig extends DateSegments {
    end: moment.Moment;
    holes?: DateConfig[];
}
