import { EventEmitter } from 'events';
import { OpConfig, ExecutionConfig, Logger } from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import moment from 'moment';
import WindowState from './window-state';
import { IDType, WildCardQuery } from '../id_reader/interfaces';

export interface ESReaderConfig extends OpConfig {
    index: string;
    field?: string;
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
    full_response: boolean;
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

// TODO: delete?
export interface DateConfig {
    start: string | moment.Moment;
    end: string | moment.Moment;
}

export interface SlicerArgs {
    opConfig: any;
    interval: ParsedInterval;
    latencyInterval?: ParsedInterval;
    executionConfig: ExecutionConfig;
    logger: Logger;
    dates: SlicerDateConfig;
    primaryRange?: DateSegments;
    id: number;
    api: elasticApi.Client;
    events: EventEmitter;
    windowState: WindowState;
}
// TODO: this is most likely wrong
export interface SlicerDateResults {
    start: string;
    end: string;
    limit: string;
    count: number;
    holes?: DateConfig[];
    wildcard?: WildCardQuery;
}

export interface ApiConfig extends ESReaderConfig {
    endpoint: string;
    token: string;
    timeout: number;
}

export type ESDateConfig = ESReaderConfig | ApiConfig;
