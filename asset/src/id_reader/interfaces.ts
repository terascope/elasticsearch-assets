import { EventEmitter } from 'events';
import { OpConfig, ExecutionConfig, Logger } from '@terascope/job-components';
import { SlicerDateResults } from '../elasticsearch_reader/interfaces';
import Reader from '../elasticsearch_reader_api/reader';

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
    key_type: IDType;
    key_range: null | string[];
    starting_key_depth: number;
    query?: string;
    fields: null | string[];
    connection: string;
    type: string | null | undefined;
}

export interface ESIDSlicerArgs {
    opConfig: ESIDReaderConfig;
    executionConfig: ExecutionConfig;
    retryData?: any;
    logger: Logger;
    range: SlicerDateResults;
    api: Reader;
    keySet: string[];
    events: EventEmitter;
    type?: string | null | undefined;
}

export interface WildCardQuery {
    field: string;
    value: string;
}

export interface ESIDSlicerResult {
    count: number;
    wildcard: WildCardQuery;
}
