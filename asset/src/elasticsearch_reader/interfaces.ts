
import {
    OpConfig,
    WorkerContext,
    ExecutionConfig,
    Logger
} from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import { IDType } from '../id_reader/interfaces';

export interface ESReaderConfig extends OpConfig {
    index: string;
    type?: string;
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

export interface SlicerArgs {
    context: WorkerContext;
    opConfig: any;
    executionConfig: ExecutionConfig;
    retryData?: any;
    logger: Logger;
    dates: any;
    id: number;
    api: elasticApi.Client;
}

export interface SlicerResults {
    start: string;
    end: string;
    count: number;
    key?: string;
}
