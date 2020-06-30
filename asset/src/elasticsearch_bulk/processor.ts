import { BatchProcessor, WorkerContext, ExecutionConfig } from '@terascope/job-components';
import { DataEntity } from '@terascope/utils';
import { ElasticSenderAPI } from '../elasticsearch_sender_api/interfaces';
import ElasticsearchSender from '../elasticsearch_sender_api/bulk_send';
import { BulkSender } from './interfaces';

export default class ElasticsearchBulk extends BatchProcessor<BulkSender> {
    limit: number;
    client!: ElasticsearchSender;
    apiManager!: ElasticSenderAPI;

    constructor(context: WorkerContext, opConfig: BulkSender, exConfig: ExecutionConfig) {
        super(context, opConfig, exConfig);
        const { size: limit } = opConfig;
        this.limit = limit;
    }

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
