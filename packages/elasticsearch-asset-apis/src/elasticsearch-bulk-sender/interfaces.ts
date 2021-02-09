import { AnyObject } from '@terascope/job-components';

export interface ElasticsearchSenderConfig {
    size: number;
    connection: string;
    index: string;
    type?: string;
    delete?: boolean;
    update?: boolean;
    update_retry_on_conflict?: number;
    update_fields?: string[];
    upsert?: boolean;
    create?: boolean;
    script_file?: string;
    script?: string;
    script_params?: AnyObject;
    _key?: string
}

export interface BulkMeta {
    _index: string;
    _type: string;
    _id: string | number;
    retry_on_conflict: number;
}

export interface IndexSpec {
    index?: AnyObject;
    create?: AnyObject;
    update?: AnyObject;
    delete?: AnyObject;
}

export interface ScriptConfig {
    file?: string;
    source?: string;
    params?: AnyObject;
}

export interface UpdateConfig {
    upsert?: AnyObject;
    doc?: AnyObject;
    script?: ScriptConfig;
}
