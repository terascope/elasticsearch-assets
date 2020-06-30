import { APIConfig, APIFactoryRegistry, AnyObject } from '@terascope/job-components';
import BulkSenderAPI from './bulk_send';

export type ElasticSenderAPI = APIFactoryRegistry<BulkSenderAPI, AnyObject>

export const DEFAULT_API_NAME = 'elasticsearch_sender_api';

export interface SenderConfig extends APIConfig {
    connection: string
    index?: string;
    size?: number;
    full_response?: boolean
}

export interface ValidSenderConfig extends APIConfig {
    connection: string
    size: number;
    index?: string;
    full_response?: boolean
}
