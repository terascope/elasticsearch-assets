import { EventEmitter } from 'events';
import { OpConfig, ExecutionConfig, Logger } from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import { SlicerDateResults } from '../elasticsearch_reader/interfaces';

export enum IDType {
    base64url = 'base64url',
    base64 = 'base64',
    hexadecimal = 'hexadecimal',
    HEXADECIMAL = 'HEXADECIMAL'
}

export interface ESIDReaderConfig extends OpConfig {
    index: string;
    size: number;
    field: string;
    full_response: boolean;
    key_type: IDType;
    key_range: null | string[];
    starting_key_depth: number;
    query?: string;
    fields: null | string[];
    connection: string;
}

export interface ESIDSlicerArgs {
    opConfig: ESIDReaderConfig;
    executionConfig: ExecutionConfig;
    retryData?: any;
    logger: Logger;
    range: SlicerDateResults;
    api: elasticApi.Client;
    keySet: string[];
    events: EventEmitter;
}

export interface WildCardQuery {
    field: string;
    value: string;
}

export interface ESIDSlicerResult {
    count: number;
    wildcard: WildCardQuery;
}
