import { OpConfig } from '@terascope/job-components';

export enum DateOptions {
    dateNow = 'dateNow',
    isoBetween = 'isoBetween',
    utcDate = 'utcDate',
    utcBetween = 'utcBetween'
}

export enum IDType {
    base64url = 'base64url',
    hexadecimal = 'hexadecimal',
    HEXADECIMAL = 'HEXADECIMAL',
}

export interface DataGenerator extends OpConfig {
    json_schema?: string;
    size: number;
    start?: string | number;
    end?: string | number;
    format?: DateOptions;
    stress_test: boolean;
    date_key: string;
    set_id?: IDType;
    id_start_key?: string;
}

export type CounterResults = null | { count: number; processed: number }
