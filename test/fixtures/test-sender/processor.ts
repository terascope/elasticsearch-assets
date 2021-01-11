import { BatchProcessor, RouteSenderAPI } from '@terascope/job-components';
import { AnyObject, DataEntity } from '@terascope/utils';

export default class TestSenderAPI extends BatchProcessor<AnyObject> {
    api!: RouteSenderAPI

    async initialize(): Promise<void> {
        this.api = this.getAPI<RouteSenderAPI>(this.opConfig.apiName);
    }

    async onBatch(data: DataEntity[]): Promise<DataEntity[]> {
        await this.api.send(data);
        return data;
    }
}
