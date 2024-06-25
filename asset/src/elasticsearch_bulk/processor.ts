import { BatchProcessor, isPromAvailable } from '@terascope/job-components';
import { DataEntity } from '@terascope/utils';
import { ElasticsearchBulkSender } from '@terascope/elasticsearch-asset-apis';
import { ElasticSenderAPI } from '../elasticsearch_sender_api/interfaces.js';
import { ElasticsearchBulkConfig } from './interfaces.js';

export default class ElasticsearchBulk extends BatchProcessor<ElasticsearchBulkConfig> {
    client!: ElasticsearchBulkSender;
    apiManager!: ElasticSenderAPI;
    private recordsWritten = 0;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiManager = this.getAPI<ElasticSenderAPI>(this.opConfig.api_name);
        this.client = await apiManager.create('bulkSender', this.opConfig);
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        if (isPromAvailable(this.context)) {
            this.context.apis.foundation.promMetrics.addGauge(
                'elasticsearch_records_written',
                'Number of records written to elasticsearch',
                ['op_name'],
                async function collect() {
                    const labels = {
                        op_name: self.opConfig._op,
                        ...self.context.apis.foundation.promMetrics.getDefaultLabels()
                    };
                    this.set(labels, self.getRecordsWritten());
                }
            );
        }
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

        this.recordsWritten += data.length;

        return data;
    }

    getRecordsWritten() {
        return this.recordsWritten;
    }
}
