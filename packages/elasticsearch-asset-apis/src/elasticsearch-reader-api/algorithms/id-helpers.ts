/* eslint-disable no-useless-escape */

import { pMap } from '@terascope/utils';
import {
    CountFn, IDSlicerRanges, IDType,
    ReaderSlice
} from '../interfaces.js';

const lowerCaseHexChars = Object.freeze([
    'a',
    'b',
    'c',
    'd',
    'e',
    'f'
]);

const upperCaseHexChars = Object.freeze([
    'A',
    'B',
    'C',
    'D',
    'E',
    'F'
]);

const lowerCaseChars = Object.freeze([
    ...lowerCaseHexChars,
    'g',
    'h',
    'i',
    'j',
    'k',
    'l',
    'm',
    'n',
    'o',
    'p',
    'q',
    'r',
    's',
    't',
    'u',
    'v',
    'w',
    'x',
    'y',
    'z',
]);

const upperCaseChars = Object.freeze([
    ...upperCaseHexChars,
    'G',
    'H',
    'I',
    'J',
    'K',
    'L',
    'M',
    'N',
    'O',
    'P',
    'Q',
    'R',
    'S',
    'T',
    'U',
    'V',
    'W',
    'X',
    'Y',
    'Z',
]);

const numerics = Object.freeze([
    '0',
    '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9',
]);

const base64URLSpecialChars = Object.freeze([
    '\-',
    '_'
]);

const base64SpecialChars = Object.freeze([
    ...base64URLSpecialChars,
    '+',
    '/'
]);

export const base64url = Object.freeze([
    ...lowerCaseChars,
    ...upperCaseChars,
    ...numerics,
    ...base64URLSpecialChars
]);

export const base64 = Object.freeze([
    ...lowerCaseChars,
    ...upperCaseChars,
    ...numerics,
    ...base64SpecialChars
]);

export const hexadecimal = Object.freeze([
    ...numerics,
    ...lowerCaseHexChars
]);

export const HEXADECIMAL = Object.freeze([
    ...numerics,
    ...upperCaseHexChars
]);

export class SplitKeyTracker {
    private ind = 0;
    private keyList: (string | number)[][] = [];
    private totalKeys: number;

    constructor(type: keyof typeof IDType) {
        if (type === IDType.base64url) {
            const specialChars = [...base64URLSpecialChars].reverse();
            // @ts-expect-error
            specialChars.__uniqueChars = true;

            this.keyList.push(
                [...lowerCaseChars].reverse(),
                [...upperCaseChars].reverse(),
                [...numerics].reverse(),
                specialChars,
            );
            this.totalKeys = base64url.length;
        } else if (type === IDType.base64) {
            const specialChars = [...base64SpecialChars].reverse();
            // @ts-expect-error
            specialChars.__uniqueChars = true;
            this.keyList.push(
                [...lowerCaseChars].reverse(),
                [...upperCaseChars].reverse(),
                [...numerics].reverse(),
                specialChars,
            );
            this.totalKeys = base64.length;
        } else if (type === IDType.hexadecimal) {
            this.keyList.push(
                [...numerics].reverse(),
                [...lowerCaseHexChars].reverse(),
            );
            this.totalKeys = hexadecimal.length;
        } else if (type === IDType.HEXADECIMAL) {
            this.keyList.push(
                [...numerics].reverse(),
                [...upperCaseHexChars].reverse(),
            );
            this.totalKeys = HEXADECIMAL.length;
        } else {
            throw new Error(`Unsupported key type ${type}`);
        }
    }

    split(size: number): string {
        if (size <= 0) {
            throw new Error('Cannot take a negative number or a zero');
        }

        let results = '';
        let keysToTake = Math.min(size, this.totalKeys - this.ind);

        // we are all done already
        if (this.ind >= this.totalKeys) return results;

        while (keysToTake > 0) {
            for (const list of this.keyList) {
                if (list.length > 0) {
                    const numInList = keysToTake < list.length ? keysToTake : list.length;
                    const firstChar = list.pop();

                    // we just popped so we increase
                    this.ind += 1;
                    keysToTake -= 1;

                    // @ts-expect-error
                    if (list.__uniqueChars) {
                        results += `${firstChar}`;

                        // no more special chars
                        if (list.length !== 0) {
                            // its 1 because we already popped once
                            for (let i = 1; i < numInList; i++) {
                                const nextChar = list.pop();
                                keysToTake -= 1;
                                this.ind += 1;

                                results += `${nextChar}`;
                            }
                        }

                        if (keysToTake <= 0) {
                            break;
                        }
                    } else {
                        let lastChar = firstChar;

                        // its 1 because we already popped once
                        for (let i = 1; i < numInList; i++) {
                            lastChar = list.pop();
                            keysToTake -= 1;
                            this.ind += 1;
                        }

                        if (firstChar === lastChar) {
                            results += `${firstChar}`;
                        } else {
                            results += `${firstChar}-${lastChar}`;
                        }

                        if (keysToTake <= 0) {
                            break;
                        }
                    }
                }
            }
        }

        return results;
    }
}

export function getKeyArray(keyType: IDType): readonly string[] {
    if (keyType === IDType.base64url) return base64url;
    if (keyType === IDType.base64) return base64;
    if (keyType === IDType.hexadecimal) return hexadecimal;
    if (keyType === IDType.HEXADECIMAL) return HEXADECIMAL;

    throw new Error('Could not find correct key type');
}

export function generateCountQueryForKeys(
    keys: readonly string[],
    rangeObj?: ReaderSlice,
): ReaderSlice {
    const query: ReaderSlice = {
        keys,
    };

    if (rangeObj) {
        query.start = rangeObj.start;
        query.end = rangeObj.end;
    }

    return query;
}

export async function determineIDSlicerRanges(
    keysArray: readonly string[],
    num: number,
    getCount: CountFn
): Promise<IDSlicerRanges> {
    const arrayLength = keysArray.length;
    const list: string[][] = [];

    for (let i = 0; i < num; i += 1) {
        list.push([]);
    }

    let counter = 0;

    for (let i = 0; i < arrayLength; i += 1) {
        list[counter].push(keysArray[i]);
        counter += 1;

        if (counter >= num) {
            counter = 0;
        }
    }
    // TODO: why is this doing a count?
    return pMap(list, async (keys) => {
        const count = await getCount(
            generateCountQueryForKeys(keys)
        );

        return {
            keys,
            count
        };
    });
}
