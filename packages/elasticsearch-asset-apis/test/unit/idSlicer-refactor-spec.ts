import { EventEmitter } from 'node:events';
import { debugLogger, times, pWhile } from '@terascope/utils';
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

    async function gatherSlices(fn: () => Promise<any>) {
        const results: any[] = [];

        await pWhile(async () => {
            const slice = await fn();
            results.push(slice);

            if (slice == null) {
                return true;
            }
        }, { timeoutMs: 100000 });

        return results;
    }

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
        const keyType = baseKey;

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
            keyType
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

        const results = await gatherSlices(slicer);

        expect(results).toEqual(expectedResults);
    });

    it('should be able to optimize the recursive call on the slice when it passes the allowed size', async () => {
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
            client,
            baseKey: IDType.hexadecimal
        });

        const expectedResults = [
            { keys: ['a'], count: 50 },
            { keys: ['b[0-9a-d]'], count: 50 },
            { keys: ['b[e-f]'], count: 50 },
            { keys: ['c'], count: 50 },
            null
        ];

        const results = await gatherSlices(slicer);

        expect(hasRecursed).toBeTrue();
        expect(results).toEqual(expectedResults);
    });

    it('should be able to optimize the recursive call back to back', async () => {
        const client = new MockClient();
        let hasRecursed = false;

        client.setSequenceData([
            { count: 50 },
            { count: 110 },
            { count: 50 },
            { count: 50 },
            { count: 110 },
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
            keySet: ['a', 'b', 'c', 'd'],
            client,
            baseKey: IDType.hexadecimal
        });

        const expectedResults = [
            { keys: ['a'], count: 50 },
            { keys: ['b[0-9a-d]'], count: 50 },
            { keys: ['b[e-f]'], count: 50 },
            { keys: ['c[0-9a-d]'], count: 50 },
            { keys: ['c[e-f]'], count: 50 },
            { keys: ['d'], count: 50 },
            null
        ];

        const results = await gatherSlices(slicer);

        expect(hasRecursed).toBeTrue();
        expect(results).toEqual(expectedResults);
    });

    it('should be able to optimize the recursive call with special chars', async () => {
        const client = new MockClient([], 50);
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
            client,
            baseKey: IDType.base64
        });

        const expectedResults = [
            { keys: ['a'], count: 50 },
            { keys: ['b[A-Za-z0-7]'], count: 50 },
            { keys: ['b[8-9\\-_\\+/]'], count: 50 },
            { keys: ['c'], count: 50 },
            null
        ];

        const results = await gatherSlices(slicer);

        expect(hasRecursed).toBeTrue();
        expect(results).toEqual(expectedResults);
    });

    it('should be able to work when the recursive size is to big as well', async () => {
        const client = new MockClient();
        let hasRecursed = false;

        client.setSequenceData([
            { count: 50 },
            { count: 110 },
            { count: 110 },
            { count: 50 },
            { count: 50 },
            { count: 110 },
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
            keySet: ['a', 'b', 'c', 'd'],
            client,
            baseKey: IDType.hexadecimal
        });

        const expectedResults = [
            { keys: ['a'], count: 50 },
            { keys: ['b[0-9a-b]'], count: 50 },
            { keys: ['b[c-f]'], count: 50 },
            { keys: ['c[0-9a-d]'], count: 50 },
            { keys: ['c[e-f]'], count: 50 },
            { keys: ['d'], count: 50 },
            null
        ];

        const results = await gatherSlices(slicer);

        expect(hasRecursed).toBeTrue();
        expect(results).toEqual(expectedResults);
    });

    it('should be able to recurse correctly with startingDepth', async () => {
        const client = new MockClient([], 50);
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
            startingKeyDepth: 1,
            keySet: ['a', 'b'],
            client,
            baseKey: IDType.hexadecimal
        });

        const expectedResults = [
            { keys: ['a0'], count: 50 },
            { keys: ['a1[0-9a-d]'], count: 50 },
            { keys: ['a1[e-f]'], count: 50 },
            { keys: ['a2'], count: 50 },
            { keys: ['a3'], count: 50 },
            { keys: ['a4'], count: 50 },
            { keys: ['a5'], count: 50 },
            { keys: ['a6'], count: 50 },
            { keys: ['a7'], count: 50 },
            { keys: ['a8'], count: 50 },
            { keys: ['a9'], count: 50 },
            { keys: ['aa'], count: 50 },
            { keys: ['ab'], count: 50 },
            { keys: ['ac'], count: 50 },
            { keys: ['ad'], count: 50 },
            { keys: ['ae'], count: 50 },
            { keys: ['af'], count: 50 },
            { keys: ['b0'], count: 50 },
            { keys: ['b1'], count: 50 },
            { keys: ['b2'], count: 50 },
            { keys: ['b3'], count: 50 },
            { keys: ['b4'], count: 50 },
            { keys: ['b5'], count: 50 },
            { keys: ['b6'], count: 50 },
            { keys: ['b7'], count: 50 },
            { keys: ['b8'], count: 50 },
            { keys: ['b9'], count: 50 },
            { keys: ['ba'], count: 50 },
            { keys: ['bb'], count: 50 },
            { keys: ['bc'], count: 50 },
            { keys: ['bd'], count: 50 },
            { keys: ['be'], count: 50 },
            { keys: ['bf'], count: 50 },
            null
        ];

        const results = await gatherSlices(slicer);

        expect(hasRecursed).toBeTrue();
        expect(results).toEqual(expectedResults);
    });
});
