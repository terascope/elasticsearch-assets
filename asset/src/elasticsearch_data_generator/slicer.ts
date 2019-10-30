
import { ParallelSlicer, SlicerFn } from '@terascope/job-components';
import { DataGenerator } from './interfaces';
import Counter from './counter';

export default class DataGeneratorSlicer extends ParallelSlicer<DataGenerator> {
    maxQueueLength() {
        return this.workersConnected + 1;
    }

    slicerQueueLength() {
        return 'QUEUE_MINIMUM_SIZE';
    }

    async newSlicer(_id: number): Promise<SlicerFn> {
        const { size } = this.opConfig;
        if (this.executionConfig.lifecycle === 'once') {
            const opSize = this.executionConfig.operations.length - 1;
            const lastOp = this.executionConfig.operations[opSize];
            const counter = new Counter(size, lastOp.size);
            // @ts-ignore TODO: fix teraslice types
            return counter.handle;
        }
        // @ts-ignore TODO: fix teraslice types
        return async () => size;
    }
}
