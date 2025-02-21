abstract class BaseChunker {
    isDone = false;
    index = 0;
    took = 0;
    total: number;
    keys: string[];

    constructor(chars: string[] | readonly string[]) {
        this.total = chars.length;
        this.keys = [...chars];
    }

    abstract chunk(numOfChars: number): string;

    split(numOfChars: number): { range: string; took: number } {
        if (this.isDone) {
            return { range: '', took: 0 };
        }

        const results = this.chunk(numOfChars);

        return { range: results, took: this.took };
    }

    commit(): void {
        if (this.took > 0) {
            this.index += this.took;
        }

        if (this.index >= this.total) {
            this.isDone = true;
        }
    }
}

export class KeyChunker extends BaseChunker {
    chunk(numOfChars: number) {
        const start = this.keys[this.index];
        const endIndex = this.index + numOfChars > this.total
            ? this.total
            : this.index + numOfChars;

        const end = this.keys[endIndex - 1];

        this.took = endIndex - this.index;

        return `${start}-${end}`;
    }
}

export class SpecialKeyChunker extends BaseChunker {
    chunk(numOfChars: number) {
        const endIndex = this.index + numOfChars > this.total
            ? this.total
            : this.index + numOfChars;

        const results = this.keys.slice(this.index, endIndex).join('');

        this.took = endIndex - this.index;

        return `${results}`;
    }
}

export type Chunker = KeyChunker | SpecialKeyChunker;
