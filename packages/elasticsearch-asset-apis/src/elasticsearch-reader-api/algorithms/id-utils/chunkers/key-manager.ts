import { IDType } from '../../../interfaces.js';
import { Chunker, KeyChunker, SpecialKeyChunker } from './key-chunkers.js';
import {
    upperCaseChars, lowerCaseChars, numerics,
    base64SpecialChars, base64URLSpecialChars,
    lowerCaseHexChars, upperCaseHexChars
} from '../keys.js';

export class SplitKeyManager {
    keyChunkers: Chunker[] = [];

    constructor(type: keyof typeof IDType) {
        if (type === IDType.base64url) {
            this.keyChunkers.push(
                new KeyChunker([...upperCaseChars]),
                new KeyChunker([...lowerCaseChars]),
                new KeyChunker([...numerics]),
                new SpecialKeyChunker([...base64URLSpecialChars]),
            );
        } else if (type === IDType.base64) {
            this.keyChunkers.push(
                new KeyChunker([...upperCaseChars]),
                new KeyChunker([...lowerCaseChars]),
                new KeyChunker([...numerics]),
                new SpecialKeyChunker([...base64SpecialChars]),
            );
        } else if (type === IDType.hexadecimal) {
            this.keyChunkers.push(
                new KeyChunker([...numerics]),
                new KeyChunker([...lowerCaseHexChars]),
            );
        } else if (type === IDType.HEXADECIMAL) {
            this.keyChunkers.push(
                new KeyChunker([...numerics]),
                new KeyChunker([...upperCaseHexChars]),
            );
        } else {
            throw new Error(`Unsupported key type ${type}`);
        }
    }

    split(num: number): string {
        let numOfChars = num;
        let results = '';

        for (const chunker of this.keyChunkers) {
            if (chunker.isDone) continue;

            const { range, took } = chunker.split(numOfChars);
            results += range;
            numOfChars -= took;

            if (numOfChars <= 0) {
                break;
            }
        }

        if (results.length) {
            results = `[${results}]`;
        }

        return results;
    }

    commit(): void {
        this.keyChunkers.forEach((chunker) => chunker.commit());
    }
}
