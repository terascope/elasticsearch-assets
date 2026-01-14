import { BatchProcessor, RouteSenderAPI } from '@terascope/job-components';
import { DataEntity } from '@terascope/core-utils';

export default class TestSenderAPI extends BatchProcessor<Record<string, any>> {
    api!: RouteSenderAPI;

    async initialize(): Promise<void> {
        this.api = this.getAPI<RouteSenderAPI>(this.opConfig.apiName);
    }

    async onBatch(data: DataEntity[]) {
        await this.api.send(data);
        return data;
    }
}
