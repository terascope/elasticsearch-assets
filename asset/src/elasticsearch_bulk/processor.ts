import { BatchProcessor } from '@terascope/job-components';
import { DataEntity } from '@terascope/utils';
import { ElasticSenderAPI } from '../elasticsearch_sender_api/interfaces';
import ElasticsearchSender from '../elasticsearch_sender_api/bulk_send';
import { ElasticsearchBulkConfig } from './interfaces';

export default class ElasticsearchBulk extends BatchProcessor<ElasticsearchBulkConfig> {
    client!: ElasticsearchSender;
    apiManager!: ElasticSenderAPI;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiManager = this.getAPI<ElasticSenderAPI>(this.opConfig.api_name);
        this.client = await apiManager.create('bulkSender', this.opConfig);
    }

    async onBatch(data: DataEntity[]): Promise<DataEntity[]> {
        if (data == null || data.length === 0) return data;
        await this.client.send(data);
        return data;
    }
}
