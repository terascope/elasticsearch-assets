import { APIConfig } from '@terascope/job-components';

export interface ReaderConfig extends APIConfig {
    connection?: string
    index?: string;
    full_response?: boolean
}

export interface ValidReaderConfig extends APIConfig {
    connection: string
    index?: string;
    full_response?: boolean
}
