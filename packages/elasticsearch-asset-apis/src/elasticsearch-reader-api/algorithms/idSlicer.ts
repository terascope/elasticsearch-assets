import { TSError } from '@terascope/utils';
import {
    IDSlicerArgs, ReaderSlice, IDSlicerResults
} from '../interfaces';
import { generateCountQueryForKeys } from './id-helpers';

export function idSlicer(args: IDSlicerArgs): () => Promise<IDSlicerResults> {
    const {
        events,
        retryData,
        range,
        baseKeyArray,
        keySet,
        countFn,
        startingKeyDepth,
        size
    } = args;

    async function determineKeySlice(
        generator: KeyGenerator,
        closePath: boolean,
        rangeObj?: ReaderSlice
    ): Promise<IDSlicerResults> {
        const data = generator.next(closePath ?? undefined);
        if (data.done) return null;

        async function getKeySlice(esQuery: ReaderSlice): Promise<IDSlicerResults> {
            const count = await countFn(esQuery);

            if (count > size) {
                events.emit('slicer:slice:recursion');
                return determineKeySlice(generator, false, rangeObj);
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
            ? generateKeyDepth(baseArray, keysArray, startingKeyDepth)
            : generateKeys(baseArray, keysArray);
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

type KeyGenerator = Generator<string, null, boolean|undefined>;

function* recurse(baseArray: readonly string[], str: string): KeyGenerator {
    for (const key of baseArray) {
        const newStr = str + key;
        const resp = yield newStr;

        if (!resp) {
            yield* recurse(baseArray, newStr);
        }
    }
    return null;
}

function* recurseDepth(
    baseArray: readonly string[],
    str: string,
    startingKeyDepth: number
): KeyGenerator {
    for (const key of baseArray) {
        const newStr = str + key;

        if (newStr.length >= startingKeyDepth) {
            const resp = yield newStr;

            if (!resp) {
                yield* recurse(baseArray, newStr);
            }
        } else {
            yield* recurse(baseArray, newStr);
        }
    }
    return null;
}

function* generateKeys(
    baseArray: readonly string[],
    keysArray: readonly string[]
): KeyGenerator {
    for (const startKey of keysArray) {
        const processKey = yield startKey;

        if (!processKey) {
            yield* recurse(baseArray, startKey);
        }
    }

    return null;
}

function* generateKeyDepth(
    baseArray: readonly string[],
    keysArray: readonly string[],
    startingKeyDepth: number
): KeyGenerator {
    for (const startKey of keysArray) {
        yield* recurseDepth(baseArray, startKey, startingKeyDepth);
    }

    return null;
}
