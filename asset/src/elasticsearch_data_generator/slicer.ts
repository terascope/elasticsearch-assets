import { Slicer, get, SlicerRecoveryData } from '@terascope/job-components';
import { DataGenerator, CounterResults } from './interfaces';
import Counter from './counter';

export default class DataGeneratorSlicer extends Slicer<DataGenerator> {
    countHandle!: () => Promise<CounterResults>

    async initialize(recoveryData: SlicerRecoveryData[]) {
        await super.initialize(recoveryData);

        const { size } = this.opConfig;
        const isPersistent = this.executionConfig.lifecycle === 'persistent';
        let alreadyProcessd: undefined|number;

        if (this.recoveryData) {
            alreadyProcessd = get(this.recoveryData[0], 'lastSlice.processed', 0);
        }

        const opListSize = this.executionConfig.operations.length - 1;
        const lastOp = this.executionConfig.operations[opListSize];

        const counter = new Counter(isPersistent, size, lastOp.size, alreadyProcessd);
        this.countHandle = counter.handle;
    }

    maxQueueLength() {
        return this.workersConnected * 3;
    }

    isRecoverable() {
        return true;
    }

    async slice() {
        return this.countHandle();
    }
}
