import {
    OpConfig,
    WorkerContext,
    ExecutionConfig,
    Logger,
    SlicerRecoveryData
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
}

export interface DateSegments {
    start: moment.Moment;
    limit: moment.Moment;
}

export interface SlicerDateConfig extends DateSegments {
    end: moment.Moment;
}

export interface StartPointConfig {
    dates: DateSegments;
    id: number;
    numOfSlicers: number;
    interval: ParsedInterval;
    recoveryData?: SlicerRecoveryData[];
}

export type ParsedInterval = [number, moment.unitOfTime.Base];

// TODO: delete?
export interface DateConfig {
    start: string;
    end: string;
}

export interface SlicerArgs {
    context: WorkerContext;
    opConfig: any;
    interval: ParsedInterval;
    delayTime?: number;
    executionConfig: ExecutionConfig;
    retryData?: any;
    logger: Logger;
    dates: SlicerDateConfig;
    id: number;
    api: elasticApi.Client;
}
// TODO: this is most likely wrong
export interface SlicerDateResults {
    start: string;
    end: string;
    limit: string;
    count: number;
    wildcard?: WildCardQuery;
}
