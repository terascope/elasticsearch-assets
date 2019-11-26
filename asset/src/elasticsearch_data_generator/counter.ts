
import { CounterResults } from './interfaces';

export default class Counter {
    numOfRecords: number;
    sliceSize: number;
    handle: () => Promise<CounterResults>

    constructor(size: number, sliceSize = 5000) {
        this.numOfRecords = size;
        this.sliceSize = sliceSize;
        this.handle = async () => {
            if (this.numOfRecords <= 0) {
                return null;
            }

            if (this.numOfRecords - this.sliceSize >= 0) {
                this.numOfRecords -= this.sliceSize;
                return { count: this.sliceSize };
            }

            const finalCount = this.numOfRecords;
            this.numOfRecords = 0;

            return { count: finalCount };
        };
    }
}
