import { CounterResults } from './interfaces.js';

export default class Counter {
    isPersistent: boolean;
    numOfRecordsProcessed: number;
    sliceSize: number;
    size: number;
    handle: () => Promise<CounterResults>;

    constructor(isPersistent: boolean, size: number, sliceSize = 5000, startingCount = 0) {
        this.numOfRecordsProcessed = startingCount;
        this.sliceSize = sliceSize;
        this.size = size;
        this.isPersistent = isPersistent;

        this.handle = async () => {
            let count = this.sliceSize;

            if (!isPersistent) {
                if (this.numOfRecordsProcessed >= this.size) {
                    return null;
                }

                if (this.numOfRecordsProcessed + this.sliceSize >= this.size) {
                    count = this.size - this.numOfRecordsProcessed;
                }
            }

            this.numOfRecordsProcessed += count;

            return { count, processed: this.numOfRecordsProcessed };
        };
    }
}
