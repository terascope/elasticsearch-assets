import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { ElasticsearchBulkSender, ElasticsearchSenderConfig } from '@terascope/elasticsearch-asset-apis';

export type ElasticSenderAPI = APIFactoryRegistry<ElasticsearchBulkSender, Record<string, any>>;

export const DEFAULT_API_NAME = 'elasticsearch_sender_api';

export interface SenderConfig extends APIConfig {
    _connection: string;
    index?: string;
    size?: number;
}

export interface ValidSenderConfig extends APIConfig {
    _connection: string;
    size: number;
    index?: string;
}

export interface ElasticsearchAPISenderConfig {
    size: number;
    _connection: string;
    _api_name: string;
    index: string;
    delete: boolean;
    update: boolean;
    update_retry_on_conflict: number;
    update_fields: string[];
    upsert: boolean;
    create: boolean;
    script_file?: string;
    script?: string;
    script_params: Record<string, any>;
    _key?: string;
}

export interface ElasticsearchSenderAPI extends ElasticsearchSenderConfig, APIConfig {}
