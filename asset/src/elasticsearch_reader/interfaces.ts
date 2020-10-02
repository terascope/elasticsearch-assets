import { EventEmitter } from 'events';
import {
    OpConfig, Logger, LifeCycle, AnyObject
} from '@terascope/job-components';
import moment from 'moment';
import WindowState from '../elasticsearch_reader_api/window-state';
import { IDType, WildCardQuery } from '../id_reader/interfaces';

export interface ESReaderConfig extends ESReaderOptions, OpConfig {
    api_name: string;
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
}

export interface InputDateSegments {
    start: moment.Moment | string | Date;
    limit: moment.Moment | string | Date;
}

export interface DateSegments {
    start: moment.Moment;
    limit: moment.Moment;
}

export interface SlicerDateConfig extends DateSegments {
    end: moment.Moment;
    holes?: DateConfig[];
}

export interface StartPointConfig {
    dates: DateSegments;
    id: number;
    numOfSlicers: number;
    interval: ParsedInterval;
    recoveryData?: SlicerDateResults[];
}

export type ParsedInterval = [number, moment.unitOfTime.Base];

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
    dates: SlicerDateConfig;
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

export interface ApiConfig extends ESReaderConfig {
    endpoint: string;
    token: string;
    timeout: number;
}

export type ESDateConfig = ESReaderConfig | ApiConfig;

export interface DetermineSliceResults {
    start: moment.Moment;
    end: moment.Moment;
    count: number;
    key?: string;
}

export interface IDReaderSlice {
    start?: string;
    end?: string;
    count: number;
    holes?: DateConfig[];
    wildcard?: WildCardQuery;
    key?: string;
}
