import { OpConfig } from '@terascope/job-components';
import { ElasticsearchSenderConfig } from '../elasticsearch_sender_api/interfaces';

export interface ElasticsearchBulkConfig extends ElasticsearchSenderConfig, OpConfig {
    api_name: string;
}
