import { BatchProcessor } from '@terascope/job-components';
import { AnyObject, DataEntity } from '@terascope/utils';

export default class TestSenderAPI extends BatchProcessor<AnyObject> {
    async onBatch(data: DataEntity[]): Promise<DataEntity[]> {
        return data;
    }
}
