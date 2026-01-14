import { APIConfig } from '@terascope/job-components';

export interface ESStateStorageConfig extends APIConfig {
    index: string;
    concurrency: number;
    source_fields: string[];
    chunk_size: number;
    persist: boolean;
    meta_key_field: string;
    _connection: string;
    cache_size: number;
}
