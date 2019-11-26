
import { AnyObject } from '@terascope/job-components';

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
