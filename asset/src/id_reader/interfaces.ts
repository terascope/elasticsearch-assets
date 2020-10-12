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
    id_field_name: string;
    key_range: null | string[];
    starting_key_depth: number;
    query?: string;
    fields: null | string[];
    connection: string;
    api_name: string;
}

export interface ESIDSlicerArgs {
    retryData?: any;
    logger: Logger;
    range?: SlicerDateResults;
    keySet: string[];
    baseKeyArray: string[];
    events: EventEmitter;
    startingKeyDepth: number;
    version: number;
    countFn: (args: AnyObject) => Promise<number>;
    type: string | null;
    idFieldName: string | null;
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
