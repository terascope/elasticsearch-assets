/* eslint-disable no-useless-escape */

import { IDSlicerRanges, IDType } from '../interfaces';

export const base64url = Object.freeze(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
    'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
    'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '\-', '_']);

export const base64 = Object.freeze(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
    'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
    'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '\-', '_', '+', '/']);

export const hexadecimal = Object.freeze(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']);

export const HEXADECIMAL = Object.freeze(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F']);

export function getKeyArray(keyType: IDType): readonly string[] {
    if (keyType === 'base64url') return base64url;
    if (keyType === 'base64') return base64;
    if (keyType === 'hexadecimal') return hexadecimal;
    if (keyType === 'HEXADECIMAL') return HEXADECIMAL;

    throw new Error('Could not find correct key type');
}

export function determineIDSlicerRanges(keysArray: readonly string[], num: number): IDSlicerRanges {
    const results: string[][] = [];
    const len = num;

    for (let i = 0; i < len; i += 1) {
        let divideNum = Math.ceil(keysArray.length / len);

        if (i === num - 1) {
            divideNum = keysArray.length;
        }

        results.push(keysArray.slice(0, divideNum));
    }

    return results;
}
