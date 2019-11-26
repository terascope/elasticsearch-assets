
import {
    OpConfig,
    WorkerContext,
    ExecutionConfig,
    Logger
} from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import moment from 'moment';
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
    time_resolution: string;
    geo_field?: string;
    geo_box_top_left?: string;
    geo_box_bottom_right?: string;
    geo_point?: string;
    geo_distance?: string;
    geo_sort_point?: string;
    geo_sort_order?: string;
    geo_sort_unit?: string;
    connection: string;
}

export type ParsedInterval = [number, moment.unitOfTime.Base];

export interface DateConfig {
    start: string;
    end: string;
    interval?: ParsedInterval;
    delayTime?: number;
}

export interface SlicerArgs {
    context: WorkerContext;
    opConfig: any;
    executionConfig: ExecutionConfig;
    retryData?: any;
    logger: Logger;
    dates: DateConfig;
    id: number;
    api: elasticApi.Client;
}

export interface SlicerDateResults {
    start: string;
    end: string;
    count: number;
    wildcard?: WildCardQuery;
}
