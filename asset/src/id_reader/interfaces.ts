import { OpConfig } from '@terascope/job-components';

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
