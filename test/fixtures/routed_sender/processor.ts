import { BatchProcessor } from '@terascope/job-components';
import { DataEntity } from '@terascope/core-utils';

export default class TestSenderAPI extends BatchProcessor<Record<string, any>> {
    async onBatch(data: DataEntity[]): Promise<DataEntity[]> {
        return data;
    }
}
