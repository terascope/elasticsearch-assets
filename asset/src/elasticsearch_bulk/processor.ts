import { BatchProcessor } from '@terascope/job-components';
import { DataEntity } from '@terascope/utils';
import { ElasticsearchBulkSender } from '@terascope/elasticsearch-asset-apis';
import { ElasticSenderAPI } from '../elasticsearch_sender_api/interfaces';
import { ElasticsearchBulkConfig } from './interfaces';

export default class ElasticsearchBulk extends BatchProcessor<ElasticsearchBulkConfig> {
    client!: ElasticsearchBulkSender;
    apiManager!: ElasticSenderAPI;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiManager = this.getAPI<ElasticSenderAPI>(this.opConfig.api_name);
        this.client = await apiManager.create('bulkSender', this.opConfig);
    }

    async onBatch(data: DataEntity[]): Promise<DataEntity[]> {
        if (data == null || data.length === 0) return data;

        await this.client.send(data);

        if (this.opConfig._dead_letter_action === 'kafka_dead_letter') {
            for (const doc of data) {
                if (doc.getMetadata('_bulk_sender_rejection') !== null) {
                    this.rejectRecord(doc, doc.getMetadata('_bulk_sender_rejection'));
                }
            }
        }

        return data;
    }
}
