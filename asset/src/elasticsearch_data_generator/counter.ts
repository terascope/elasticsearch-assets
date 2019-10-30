
export default class Counter {
    numOfRecords: number;
    sliceSize: number;

    constructor(size: number, sliceSize = 5000) {
        this.numOfRecords = size;
        this.sliceSize = sliceSize;
    }

    async handle() {
        if (this.numOfRecords <= 0) {
            return null;
        }

        if (this.numOfRecords - this.sliceSize >= 0) {
            this.numOfRecords -= this.sliceSize;
            return this.sliceSize;
        }

        const finalCount = this.numOfRecords;
        this.numOfRecords = 0;
        return finalCount;
    }
}
