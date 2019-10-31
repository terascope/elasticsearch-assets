
import {
    OpConfig,
    WorkerContext,
    ExecutionConfig,
    Logger
} from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import { SlicerResults } from '../elasticsearch_reader/interfaces';

export enum IDType {
    base64url = 'base64url',
    base64 = 'base64',
    hexadecimal = 'hexadecimal',
    HEXADECIMAL = 'HEXADECIMAL'
}

export interface ESIDReaderConfig extends OpConfig {
    index: string;
    type: string;
    size: number;
    full_response: boolean;
    key_type: IDType;
    key_range: null | string[];
    starting_key_depth: number;
    query?: string;
    fields: null | string[];
    connection: string;
}

export interface ESIDSlicerArgs {
    context: WorkerContext;
    opConfig: ESIDReaderConfig;
    executionConfig: ExecutionConfig;
    retryData?: any;
    logger: Logger;
    range: SlicerResults;
    api: elasticApi.Client;
    keySet: string[];
}
