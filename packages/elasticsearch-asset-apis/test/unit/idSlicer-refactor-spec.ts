import { EventEmitter } from 'node:events';
import { debugLogger, times } from '@terascope/utils';
import {
    idSlicer, getKeyArray, IDType,
    idSlicerOptimized
} from '../../src/index.js';
import { MockClient } from '../helpers/index.js';

describe('Refactored idSlicer', () => {
    const logger = debugLogger('dateSlicerFn');
    let events: EventEmitter;

    beforeEach(() => {
        events = new EventEmitter();
    });

    interface IdSlicerTestArgs {
        client?: MockClient;
        size: number;
        startingKeyDepth?: number;
        baseKey?: IDType;
        keySet?: string[];
        optimized?: boolean;
    }

    function makeIdSlicer({
        client: _client,
        size,
        startingKeyDepth = 0,
        baseKey = IDType.hexadecimal,
        keySet: _keySet,
        optimized = true
    }: IdSlicerTestArgs) {
        let client = _client;
        const baseKeyArray = getKeyArray(baseKey);
        const keySet = _keySet ?? baseKeyArray;

        if (client == null) {
            client = new MockClient();
            client.setSequenceData(times(50, () => ({ count: 100, '@timestamp': new Date() })));
        }

        async function countFn() {
            const data = await client!.search({ index: 'test' });
            return data.hits.total;
        }

        const slicerArgs = {
            events,
            logger,
            keySet,
            baseKeyArray,
            startingKeyDepth,
            countFn,
            size,
        } as unknown as any;

        if (optimized) {
            return idSlicerOptimized(slicerArgs);
        }

        return idSlicer(slicerArgs);
    }

    it('idSlicer can return a function that makes slices', async () => {
        const slicer = makeIdSlicer({
            size: 1000,
            startingKeyDepth: 0,
        });

        expect(slicer).toBeFunction();

        const slice = await slicer();

        expect(slice).toMatchObject({ keys: ['0'], count: 100 });
    });

    it('idSlicer will return null after finishing slicing', async () => {
        const slicer = makeIdSlicer({
            size: 1000,
            startingKeyDepth: 0,
            keySet: ['a', 'b', 'c']
        });

        const expectedResults = [
            {
                keys: ['a'],
                count: 100
            },
            {
                keys: ['b'],
                count: 100
            },
            {
                keys: ['c'],
                count: 100
            },
            null
        ];

        const results = [];

        for (let i = 0; i < 4; i++) {
            const slice = await slicer();
            results.push(slice);
        }

        expect(results).toEqual(expectedResults);
    });

    fit('should be able to optimize the recursive call on the slice when it passes the allowed size', async () => {
        const client = new MockClient();
        let hasRecursed = false;

        client.setSequenceData([
            { count: 50 },
            { count: 110 },
            { count: 50 },
            { count: 50 },
            { count: 50 },
            { count: 50 },

        ]);

        events.on('slicer:slice:recursion', () => {
            hasRecursed = true;
        });

        const slicer = makeIdSlicer({
            size: 100,
            startingKeyDepth: 0,
            keySet: ['a', 'b', 'c'],
            client
        });

        const slice1 = await slicer();
        const slice2 = await slicer();
        const slice3 = await slicer();
        const slice4 = await slicer();
        console.dir({ slice1, slice2, slice3, slice4 })

        expect(hasRecursed).toBeTrue();
    });
});
