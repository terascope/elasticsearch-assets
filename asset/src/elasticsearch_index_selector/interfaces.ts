import { AnyObject, DataEntity } from '@terascope/utils';

export interface IndexSelectorConfig {
    index: string;
    type?: string;
    preserve_id: boolean;
    id_field?: string;
    timeseries?: string;
    index_prefix?: string;
    date_field?: string;
    delete: boolean;
    update: boolean;
    update_retry_on_conflict: number;
    update_fields: string[];
    upsert: boolean;
    create: boolean;
    script_file?: string;
    script?: string;
    script_params: AnyObject;
}

export const INDEX_META = 'elasticsearch:index:metadata';
export const MUTATE_META = 'elasticsearch:mutate:metadata';

export interface BulkMeta {
    _index: string;
    _type: string;
    _id: string | number;
    retry_on_conflict: number;
}

export interface IndexSpec extends DataEntity {
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

export interface UpdateConfig extends DataEntity {
    upsert?: AnyObject;
    doc?: AnyObject;
    script?: ScriptConfig;
}
