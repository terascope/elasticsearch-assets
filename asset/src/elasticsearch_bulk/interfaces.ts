import { OpConfig } from '@terascope/job-components';
import { ElasticsearchAPISenderConfig } from '../elasticsearch_sender_api/interfaces.js';

export interface ElasticsearchBulkConfig extends ElasticsearchAPISenderConfig, Omit<OpConfig, 'connection'> {
    api_name: string;
}
