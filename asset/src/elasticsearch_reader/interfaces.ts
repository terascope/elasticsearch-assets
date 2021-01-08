import { EventEmitter } from 'events';
import {
    OpConfig,
    Logger,
    LifeCycle,
    AnyObject
} from '@terascope/job-components';
import moment from 'moment';
import { DataTypeConfig } from '@terascope/types';
import WindowState from '../elasticsearch_reader_api/window-state';
import { IDType, WildCardQuery } from '../id_reader/interfaces';
import { DateSegments, ParsedInterval } from '../../../packages/elasticsearch-asset-apis/interfaces';

export interface ESReaderConfig extends ESReaderOptions, OpConfig {
    api_name: string;
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

export type ESDateConfig = ESReaderConfig | ApiConfig;
