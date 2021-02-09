import { OpConfig } from '@terascope/job-components';
import { ElasticsearchAPISenderConfig } from '../elasticsearch_sender_api/interfaces';

export interface ElasticsearchBulkConfig extends ElasticsearchAPISenderConfig, OpConfig {
    api_name: string;
}
