import { TSError } from '@terascope/utils';
import {
    IDSlicerArgs, SlicerDateResults, IDReaderSlice, IDSlicerResults
} from '../interfaces';

export function idSlicer(args: IDSlicerArgs): () => Promise<IDSlicerResults> {
    const {
        events,
        retryData,
        range,
        baseKeyArray,
        keySet,
        version,
        countFn,
        startingKeyDepth,
        type,
        idFieldName,
        size
    } = args;

    async function determineKeySlice(
        generator: any,
        closePath: boolean,
        rangeObj?: SlicerDateResults
    ): Promise<IDReaderSlice| null> {
        let data;
        if (closePath) {
            data = generator.next(closePath);
        } else {
            data = generator.next();
        }

        if (data.done) return null;

        const query: Partial<IDReaderSlice> = {};

        if (rangeObj) {
            query.start = rangeObj.start;
            query.end = rangeObj.end;
        }

        if (version >= 6) {
            const fieldValue = idFieldName as string;
            query.wildcard = { field: fieldValue, value: `${data.value}*` };
        } else {
            query.key = `${type}#${data.value}*`;
        }

        async function getKeySlice(esQuery: {
            start?: string;
            end?: string;
        }): Promise<IDReaderSlice | null> {
            const count = await countFn(esQuery);

            if (count > size) {
                events.emit('slicer:slice:recursion');
                return determineKeySlice(generator, false, rangeObj);
            }

            if (count !== 0) {
                // the closing of this path happens at keyGenerator
                return { ...esQuery, count };
            }

            // if count is zero then close path to prevent further iteration
            return determineKeySlice(generator, true, rangeObj);
        }

        return getKeySlice(query);
    }

    function keyGenerator(
        baseArray: readonly string[],
        keysArray: readonly string[],
        retryKey?: string,
        dateRange?: SlicerDateResults
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
                const error = new TSError(err, {
                    reason: 'Failure to make slice for id_slicer'
                });
                return Promise.reject(error);
            }
        };
    }

    return keyGenerator(baseKeyArray, keySet, retryData, range);
}

// return true if the keys do not match
function compareKeys(key: string, retryKey: string) {
    for (let i = 0; i < key.length; i += 1) {
        if (key[i] !== retryKey[i]) {
            return true;
        }
    }
    return false;
}

function* recurse(baseArray: readonly string[], str: string): Generator<string> {
    for (const key of baseArray) {
        const newStr = str + key;
        const resp = yield newStr;

        if (!resp) {
            yield* recurse(baseArray, newStr);
        }
    }
}

function* recurseDepth(
    baseArray: readonly string[],
    str: string,
    startingKeyDepth: number
): Generator<string> {
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
}

function* generateKeys(
    baseArray: readonly string[],
    keysArray: readonly string[]
): Generator<string> {
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
): Generator<string> {
    for (const startKey of keysArray) {
        yield* recurseDepth(baseArray, startKey, startingKeyDepth);
    }

    return null;
}
