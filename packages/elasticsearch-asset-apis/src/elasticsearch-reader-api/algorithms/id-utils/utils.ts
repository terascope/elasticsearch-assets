import { pMap } from '@terascope/utils';
import {
    CountFn, IDSlicerRanges, ReaderSlice
} from '../../interfaces.js';

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
