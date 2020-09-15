import { APIConfig, APIFactoryRegistry, AnyObject } from '@terascope/job-components';
import BulkSenderAPI from './bulk_send';

export type ElasticSenderAPI = APIFactoryRegistry<BulkSenderAPI, AnyObject>

export const DEFAULT_API_NAME = 'elasticsearch_sender_api';

export interface SenderConfig extends APIConfig {
    connection: string
    index?: string;
    size?: number;
}

export interface ValidSenderConfig extends APIConfig {
    connection: string
    size: number;
    index?: string;
}

export interface ElasticsearchSenderConfig {
    size: number;
    connection: string;
    api_name: string;
    index: string;
    type?: string;
    delete: boolean;
    update: boolean;
    update_retry_on_conflict: number;
    update_fields: string[];
    upsert: boolean;
    create: boolean;
    script_file?: string;
    script?: string;
    script_params: AnyObject;
    _key?: string
}

export interface ElasticsearchSenderApi extends ElasticsearchSenderConfig, APIConfig {}

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
