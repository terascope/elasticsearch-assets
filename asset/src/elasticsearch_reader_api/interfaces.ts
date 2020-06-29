import { APIConfig } from '@terascope/job-components';

export const DEFAULT_API_NAME = 'elasticsearch_reader_api';

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
