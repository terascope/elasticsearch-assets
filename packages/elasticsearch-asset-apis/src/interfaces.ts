import type { LifeCycle, SlicerRecoveryData } from '@terascope/job-components';
import { EventEmitter } from 'events';
import { AnyObject, Logger } from '@terascope/utils';
import { DataTypeConfig, xLuceneVariables } from '@terascope/types';
import { WindowState } from './window-state';

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
export interface DateSegments {
    start: moment.Moment;
    limit: moment.Moment;
}

export interface IDSlicerArgs {
    retryData?: any;
    logger: Logger;
    range?: SlicerDateResults;
    keySet: string[];
    baseKeyArray: string[];
    events: EventEmitter;
    startingKeyDepth: number;
    version: number;
    countFn: (args: AnyObject) => Promise<number>;
    type: string | null;
    idFieldName: string | null;
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
    opConfig: any;
    interval: ParsedInterval;
    latencyInterval?: ParsedInterval;
    lifecycle: LifeCycle;
    numOfSlicers: number;
    logger: Logger;
    dates: SlicerDates;
    primaryRange?: DateSegments;
    id: number;
    events: EventEmitter;
    windowState: WindowState;
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

export type ParsedInterval = [number, moment.unitOfTime.Base];

export interface DateSlicerArgs {
    lifecycle: LifeCycle,
    slicerID: number,
    numOfSlicers: number,
    recoveryData?: SlicerRecoveryData[];
    windowState?: WindowState,
    startTime?: Date | string
    hook?: (args: AnyObject) => Promise<void>
}

export interface DateSlicerConfig {
    lifecycle: LifeCycle,
    slicerID: number,
    numOfSlicers: number,
    recoveryData: SlicerRecoveryData[],
    windowState?: WindowState,
    startTime?: Date | string,
    hook?: (args: AnyObject) => Promise<void>
}

export interface StartPointConfig {
    dates: DateSegments;
    id: number;
    numOfSlicers: number;
    interval: ParsedInterval;
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
    start: null | string;
    end: null | string;
    interval: string;
    preserve_id: boolean;
    date_field_name: string;
    query?: string;
    fields: null | string[];
    delay: string;
    subslice_by_key: boolean;
    subslice_key_threshold: number;
    key_type: IDType;
    type: string | null;
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
    full_response?: boolean;
    headers?: AnyObject
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

export interface ElasticsearchSenderConfig {
    size: number;
    connection: string;
    index: string;
    type?: string;
    delete?: boolean;
    update?: boolean;
    update_retry_on_conflict?: number;
    update_fields?: string[];
    upsert?: boolean;
    create?: boolean;
    script_file?: string;
    script?: string;
    script_params?: AnyObject;
    _key?: string
}

export interface BulkMeta {
    _index: string;
    _type: string;
    _id: string | number;
    retry_on_conflict: number;
}

export interface IndexSpec {
    index?: AnyObject;
    create?: AnyObject;
    update?: AnyObject;
    delete?: AnyObject;
}

export interface ScriptConfig {
    file?: string;
    source?: string;
    params?: AnyObject;
}

export interface UpdateConfig {
    upsert?: AnyObject;
    doc?: AnyObject;
    script?: ScriptConfig;
}
