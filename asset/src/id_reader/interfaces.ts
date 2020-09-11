import { EventEmitter } from 'events';
import { OpConfig, AnyObject, Logger } from '@terascope/job-components';
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
    key_range: null | string[];
    starting_key_depth: number;
    query?: string;
    fields: null | string[];
    connection: string;
}

export interface ESIDSlicerArgs {
    retryData?: any;
    logger: Logger;
    range?: SlicerDateResults;
    keySet: string[];
    baseKeyArray: string[];
    events: EventEmitter;
    starting_key_depth: number;
    version: number;
    countFn: (args: AnyObject) => Promise<number>;
    type: string | null;
    field: string | null;
    size: number;
}

export interface WildCardQuery {
    field: string;
    value: string;
}

export interface ESIDSlicerResult {
    count: number;
    wildcard: WildCardQuery;
}
