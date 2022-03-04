/* eslint-disable no-useless-escape */

import pMap from 'p-map';
import {
    CountFn, IDSlicerRanges, IDType, ReaderSlice
} from '../interfaces';

export const base64url = Object.freeze(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
    'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
    'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '\-', '_']);

export const base64 = Object.freeze(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
    'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
    'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '\-', '_', '+', '/']);

export const hexadecimal = Object.freeze(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']);

export const HEXADECIMAL = Object.freeze(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F']);

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
        // console.log('what counter', counter, keysArray[i])
        list[counter].push(keysArray[i]);
        counter += 1;

        if (counter >= num) {
            counter = 0;
        }
    }

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
