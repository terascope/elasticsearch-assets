import { TSError, SlicerFn, AnyObject } from '@terascope/job-components';
import { ESIDSlicerArgs } from './interfaces';
import { getKeyArray } from './helpers';
import { retryModule } from '../elasticsearch_reader/elasticsearch_date_range/helpers';
import { SlicerDateResults, IDReaderSlice } from '../elasticsearch_reader/interfaces';

export default function newSlicer(args: ESIDSlicerArgs): SlicerFn {
    const {
        events,
        opConfig,
        executionConfig,
        retryData,
        logger,
        api,
        range,
        keySet,
    } = args;
    const baseKeyArray = getKeyArray(opConfig.key_type);
    const startingKeyDepth = opConfig.starting_key_depth;
    const { version } = api;
    const retryError = retryModule(logger, executionConfig.max_retries);

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
            query.wildcard = { field: opConfig.field, value: `${data.value}*` };
        } else {
            query.key = `${opConfig.type}#${data.value}*`;
        }

        async function getKeySlice(esQuery: AnyObject): Promise<IDReaderSlice | null> {
            let count: number;

            try {
                count = await api.count(query);
            } catch (err) {
                return retryError(esQuery, err, getKeySlice, esQuery);
            }

            if (count > opConfig.size) {
                events.emit('slicer:slice:recursion');
                return determineKeySlice(generator, false, rangeObj);
            }

            if (count !== 0) {
                // the closing of this path happens at keyGenerator
                return { ...query, count };
            }

            // if count is zero then close path to prevent further iteration
            return determineKeySlice(generator, true, rangeObj);
        }

        return getKeySlice(query);
    }

    function keyGenerator(
        baseArray: string[],
        keysArray: string[],
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

function* recurse(baseArray: string[], str: string): Generator<string> {
    for (const key of baseArray) {
        const newStr = str + key;
        const resp = yield newStr;

        if (!resp) {
            yield* recurse(baseArray, newStr);
        }
    }
}

function* recurseDepth(baseArray: string[], str: string, startingKeyDepth: number) {
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

function* generateKeys(baseArray: string[], keysArray: string[]) {
    for (const startKey of keysArray) {
        const processKey = yield startKey;

        if (!processKey) {
            yield* recurse(baseArray, startKey);
        }
    }

    return null;
}

function* generateKeyDepth(baseArray: string[], keysArray: string[], startingKeyDepth: number) {
    for (const startKey of keysArray) {
        yield* recurseDepth(baseArray, startKey, startingKeyDepth);
    }

    return null;
}
