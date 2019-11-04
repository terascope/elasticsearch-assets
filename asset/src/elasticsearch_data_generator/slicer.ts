
import {
    Slicer, WorkerContext, ExecutionConfig
} from '@terascope/job-components';
import { DataGenerator, CounterResults } from './interfaces';
import Counter from './counter';

export default class DataGeneratorSlicer extends Slicer<DataGenerator> {
    countHandle: () => Promise<CounterResults>
    constructor(
        context: WorkerContext,
        opConfig: DataGenerator,
        executionConfig: ExecutionConfig
    ) {
        super(context, opConfig, executionConfig);
        const { size } = this.opConfig;
        if (this.executionConfig.lifecycle === 'once') {
            const opListSize = this.executionConfig.operations.length - 1;
            const lastOp = this.executionConfig.operations[opListSize];
            const counter = new Counter(size, lastOp.size);
            this.countHandle = counter.handle;
        } else {
            this.countHandle = async () => ({ count: size });
        }
    }

    slicerQueueLength() {
        return 'QUEUE_MINIMUM_SIZE';
    }

    async slice() {
        return this.countHandle();
    }
}
