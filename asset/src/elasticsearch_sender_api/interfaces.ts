import { APIConfig, APIFactoryRegistry, AnyObject } from '@terascope/job-components';
import { ElasticsearchSender, ElasticsearchSenderConfig } from '@terascope/elasticsearch-asset-apis';

export type ElasticSenderAPI = APIFactoryRegistry<ElasticsearchSender, AnyObject>

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

export interface ElasticsearchAPISenderConfig {
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

export interface ElasticsearchSenderAPI extends ElasticsearchSenderConfig, APIConfig {}
