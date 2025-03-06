import { TSError, isNumber } from '@terascope/utils';
import {
    IDSlicerArgs, ReaderSlice, IDSlicerResults,
    IDType
} from '../interfaces.js';
import { generateCountQueryForKeys, SplitKeyManager } from './id-utils/index.js';

export function idSlicerOptimized(args: IDSlicerArgs): () => Promise<IDSlicerResults> {
    const {
        events,
        retryData,
        range,
        baseKeyArray,
        keySet,
        countFn,
        startingKeyDepth,
        size,
        keyType
    } = args;

    const createRatio = createRatioFN(size, baseKeyArray.length);

    async function determineKeySlice(
        generator: KeyGenerator,
        genResponse: boolean | number,
        rangeObj?: ReaderSlice
    ): Promise<IDSlicerResults> {
        const data = generator.next(genResponse);
        if (data.done) return null;

        async function getKeySlice(esQuery: ReaderSlice): Promise<IDSlicerResults> {
            const count = await countFn(esQuery);

            if (count > size) {
                events.emit('slicer:slice:recursion');
                const ratio = createRatio(count);

                return determineKeySlice(generator, ratio, rangeObj);
            }

            if (count !== 0) {
                // the closing of this path happens at keyGenerator
                return {
                    ...esQuery,
                    count
                };
            }

            // if count is zero then close path to prevent further iteration
            return determineKeySlice(generator, true, rangeObj);
        }

        return getKeySlice(
            generateCountQueryForKeys(
                [data.value],
                rangeObj
            )
        );
    }

    function keyGenerator(
        baseArray: readonly string[],
        keysArray: readonly string[],
        retryKey?: string,
        dateRange?: ReaderSlice
    ) {
        // if there is a starting depth, use the key depth generator, if not use default generator
        const gen = startingKeyDepth > 0
            ? generateKeyDepth(baseArray, keysArray, startingKeyDepth, keyType)
            : generateKeys(baseArray, keysArray, keyType);
        let closePath = false;

        if (retryKey) {
            let foundKey = false;
            let skipKey = false;
            closePath = true;

            while (!foundKey) {
                const key = gen.next(skipKey).value;
                if (key === null) {
                    foundKey = true;
                } else {
                    // reset skipKey if used
                    if (skipKey) {
                        skipKey = false;
                    }
                    if (compareKeys(key, retryKey)) {
                        skipKey = true;
                    } else if (key === retryKey) {
                        foundKey = true;
                    }
                }
            }
        }

        return async function slicer() {
            try {
                const results = await determineKeySlice(gen, closePath, dateRange);
                closePath = true;
                return results;
            } catch (err) {
                throw new TSError(err, {
                    reason: 'Failure to make slice for id_slicer'
                });
            }
        };
    }

    return keyGenerator(baseKeyArray, keySet, retryData, range);
}

// return true if the keys do not match
function compareKeys(key: string, retryKey: string): boolean {
    for (let i = 0; i < key.length; i += 1) {
        if (key[i] !== retryKey[i]) {
            return true;
        }
    }

    return false;
}

type KeyGenerator = Generator<string, null, boolean | undefined | number>;

export function* recurse(
    baseArray: readonly string[],
    str: string,
    keyType: IDType
): KeyGenerator {
    for (const key of baseArray) {
        const newStr = str + key;
        const resp = yield newStr;

        // false == go deeper, true == all done, number = split keys
        if (resp === false) {
            yield * recurse(baseArray, newStr, keyType);
        } else if (isNumber(resp)) {
            yield * splitKeys(baseArray, newStr, keyType, resp);
        }
    }

    return null;
}

export function* splitKeys(
    baseArray: readonly string[],
    str: string,
    keyType: IDType,
    ratio: number,
): KeyGenerator {
    const tracker = new SplitKeyManager(keyType);

    let chunkSize = ratio;
    let isLimitOfSplitting = false;
    let isDone = false;

    while (!isDone) {
        const split = tracker.split(chunkSize);

        if (split.length === 0) {
            isDone = true;
            return null;
        }

        if (split.length === 1) {
            isLimitOfSplitting = true;
        }

        const response = yield `${str}${split}`;

        // if its a number, the current split is too big
        if (isNumber(response)) {
            if (isLimitOfSplitting) {
                // if we have to split further and we are at limit, do normal recursion
                // on that key, split is just a single char here
                yield * recurse(baseArray, `${str}${split}`, keyType);
                isDone = true;
            } else {
                // change to chunk size, do not commit as we need to redo
                const newChunkSize = Math.max(
                    Math.floor(ratio * (response / baseArray.length)),
                    1
                );

                // if the old size is less or equal to last calculation, decrease further
                // this could happen if count size and new ratio are two close together,
                // the Math.floor will make it the same index number in the array
                if (chunkSize <= newChunkSize) {
                    chunkSize -= 1;
                } else {
                    chunkSize = newChunkSize;
                }
            }
        } else {
            tracker.commit();
        }
    }

    return null;
}

function* recurseDepth(
    baseArray: readonly string[],
    str: string,
    startingKeyDepth: number,
    keyType: IDType
): KeyGenerator {
    for (const key of baseArray) {
        const newStr = str + key;

        if (newStr.length >= startingKeyDepth) {
            const response = yield newStr;

            if (response === false) {
                yield * recurse(baseArray, newStr, keyType);
            } else if (isNumber(response)) {
                yield * splitKeys(baseArray, newStr, keyType, response);
            }
        } else {
            yield * recurse(baseArray, newStr, keyType);
        }
    }

    return null;
}

export function* generateKeys(
    baseArray: readonly string[],
    keysArray: readonly string[],
    keyType: IDType
): KeyGenerator {
    for (const startKey of keysArray) {
        const response = yield startKey;
        // false == go deeper, true == all done, number = split keys
        if (response === false) {
            yield * recurse(baseArray, startKey, keyType);
        } else if (isNumber(response)) {
            yield * splitKeys(baseArray, startKey, keyType, response);
        }
    }

    return null;
}

function* generateKeyDepth(
    baseArray: readonly string[],
    keysArray: readonly string[],
    startingKeyDepth: number,
    keyType: IDType
): KeyGenerator {
    for (const startKey of keysArray) {
        yield * recurseDepth(baseArray, startKey, startingKeyDepth, keyType);
    }

    return null;
}

export function createRatioFN(size: number, arrayLength: number) {
    const limit = size * arrayLength;

    return function _createRatio(count: number): number | boolean {
        if (count >= limit) {
            // false means do a regular key recursion
            return false;
        }
        const ratio = Math.floor(arrayLength * (size / count));
        // if we cant even group by two then there is nothing to be gained
        // over the regular recurse calling
        if (ratio <= 1) {
            return false;
        }

        return ratio;
    };
}
