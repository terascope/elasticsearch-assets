import { SlicerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { SlicerRecoveryData, debugLogger, TestClientConfig } from '@terascope/job-components';
import { ElasticsearchTestHelpers } from '@terascope/opensearch-client';
import {
    TEST_INDEX_PREFIX,
    cleanupIndex,
    makeClient,
    populateIndex,
    waitForData,
} from '../helpers/index.js';

describe('id_reader slicer', () => {
    const apiReaderIndex = `${TEST_INDEX_PREFIX}_id_slicer`;
    const docType = '_doc';
    const field = 'uuid';
    const evenSpread = ElasticsearchTestHelpers.EvenDateData;
    const logger = debugLogger('test-logger');
    // for compatibility tests for older elasticsearch version,
    // we make the _id of the record the same as its uuid field
    const bulkData = evenSpread.data;

    let harness: SlicerTestHarness;
    let clients: TestClientConfig[];
    let esClient: any;

    beforeAll(async () => {
        esClient = await makeClient();
        await cleanupIndex(esClient, `${apiReaderIndex}*`);
        await populateIndex(esClient, apiReaderIndex, evenSpread.EvenDataType, bulkData, docType);
        await waitForData(esClient, apiReaderIndex, bulkData.length);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, `${apiReaderIndex}*`);
    });

    beforeEach(() => {
        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: esClient,
                    logger
                })
            }
        ];
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    const defaults = {
        _op: 'id_reader',
        field,
        index: apiReaderIndex,
        type: docType
    };

    async function makeSlicerTest(
        opConfig: Record<string, any> = {},
        numOfSlicers = 1,
        recoveryData: SlicerRecoveryData[] | undefined = undefined
    ) {
        const config = Object.assign({}, defaults, opConfig);
        const job = newTestJobConfig({
            analytics: true,
            slicers: numOfSlicers,
            operations: [
                config,
                {
                    _op: 'noop'
                }
            ]
        });
        harness = new SlicerTestHarness(job, { clients });
        await harness.initialize(recoveryData);
        return harness;
    }

    it('can create a slicer', async () => {
        const test = await makeSlicerTest();
        const slicer = test.slicer();

        expect(slicer.slicers()).toEqual(1);
    });

    it('can create multiple slicers', async () => {
        const test = await makeSlicerTest({}, 2);
        const slicer = test.slicer();

        expect(slicer.slicers()).toEqual(2);
    });

    it('a single slicer can produces values', async () => {
        const test = await makeSlicerTest();
        const results = await test.getAllSlices();

        // null shows we are done, we remove to compare objects
        expect(results.pop()).toBeNull();

        const expectedResults = [
            { keys: ['a'], count: 58 },
            { keys: ['b'], count: 82 },
            { keys: ['c'], count: 64 },
            { keys: ['d'], count: 49 },
            { keys: ['e'], count: 59 },
            { keys: ['f'], count: 51 },
            { keys: ['0'], count: 70 },
            { keys: ['1'], count: 55 },
            { keys: ['2'], count: 55 },
            { keys: ['3'], count: 54 },
            { keys: ['4'], count: 68 },
            { keys: ['5'], count: 64 },
            { keys: ['6'], count: 52 },
            { keys: ['7'], count: 80 },
            { keys: ['8'], count: 75 },
            { keys: ['9'], count: 64 },
        ];

        results.forEach((result, index) => {
            expect(result).toMatchObject(expectedResults[index]);
        });
    });

    it('can call on a subset of keys', async () => {
        const test = await makeSlicerTest({ key_range: ['a', 'b'] });
        const results = await test.getAllSlices();

        // null shows we are done, we remove to compare objects
        expect(results.pop()).toBeNull();

        const expectedResults = [
            { keys: ['a'], count: 58 },
            { keys: ['b'], count: 82 },
        ];

        results.forEach((result, index) => {
            expect(result).toMatchObject(expectedResults[index]);
        });
    });

    it('can fit slices down to size', async () => {
        const opConfig = {
            _op: 'id_reader',
            key_type: 'hexadecimal',
            key_range: ['a'],
            field,
            index: apiReaderIndex,
            size: 40
        };

        const test = await makeSlicerTest(opConfig);
        const results = await test.getAllSlices();

        expect(results.pop()).toBeNull();

        const expectedResults = [
            { keys: ['a0'], count: 5 },
            { keys: ['a1'], count: 7 },
            { keys: ['a3'], count: 2 },
            { keys: ['a4'], count: 3 },
            { keys: ['a5'], count: 3 },
            { keys: ['a6'], count: 3 },
            { keys: ['a7'], count: 4 },
            { keys: ['a8'], count: 5 },
            { keys: ['a9'], count: 8 },
            { keys: ['aa'], count: 3 },
            { keys: ['ab'], count: 3 },
            { keys: ['ac'], count: 3 },
            { keys: ['ad'], count: 4 },
            { keys: ['ae'], count: 4 },
            { keys: ['af'], count: 1 },
        ];

        results.forEach((result, index) => {
            expect(result).toMatchObject(expectedResults[index]);
        });
    });

    it('can fit slices down to size with recursive optimizations', async () => {
        const opConfig = {
            _op: 'id_reader',
            key_type: 'hexadecimal',
            key_range: ['a'],
            field,
            index: apiReaderIndex,
            size: 20,
            recurse_optimization: true
        };

        const test = await makeSlicerTest(opConfig);
        const results = await test.getAllSlices();

        expect(results.pop()).toBeNull();

        // this double recurses, first it takes 5 keys, then switches to taking 4 keys
        const expectedResults = [
            { keys: ['a[0-4]'], count: 17 },
            { keys: ['a[5-8]'], count: 15 },
            { keys: ['a[9a-c]'], count: 17 },
            { keys: ['a[d-f]'], count: 9 },
        ];

        expect(results.length).toBeGreaterThan(1);

        results.forEach((result, index) => {
            expect(result).toMatchObject(expectedResults[index]);
        });
    });

    it('can fit slices down to size with recursive optimizations for the entire index', async () => {
        const opConfig = {
            _op: 'id_reader',
            key_type: 'hexadecimal',
            field,
            index: apiReaderIndex,
            size: 20,
            recurse_optimization: true
        };

        const test = await makeSlicerTest(opConfig);
        const results = await test.getAllSlices();

        expect(results.pop()).toBeNull();

        // this double recurses, first it takes 5 keys, then switches to taking 4 keys
        const expectedResults = [
            { keys: ['0[0-3]'], count: 16 },
            { keys: ['0[4-7]'], count: 16 },
            { keys: ['0[8-9a-b]'], count: 19 },
            { keys: ['0[c-f]'], count: 19 },
            { keys: ['1[0-3]'], count: 18 },
            { keys: ['1[4-7]'], count: 10 },
            { keys: ['1[8-9a-b]'], count: 11 },
            { keys: ['1[c-f]'], count: 16 },
            { keys: ['2[0-4]'], count: 20 },
            { keys: ['2[5-9]'], count: 14 },
            { keys: ['2[a-e]'], count: 17 },
            { keys: ['2[f]'], count: 4 },
            { keys: ['3[0-4]'], count: 5 },
            { keys: ['3[5-8]'], count: 17 },
            { keys: ['3[9a-b]'], count: 16 },
            { keys: ['3[c-e]'], count: 12 },
            { keys: ['3[f]'], count: 4 },
            { keys: ['4[0-3]'], count: 14 },
            { keys: ['4[4-7]'], count: 17 },
            { keys: ['4[8-9]'], count: 12 },
            { keys: ['4[a-b]'], count: 15 },
            { keys: ['4[c-d]'], count: 5 },
            { keys: ['4[e-f]'], count: 5 },
            { keys: ['5[0-3]'], count: 18 },
            { keys: ['5[4-7]'], count: 17 },
            { keys: ['5[8-9a-b]'], count: 11 },
            { keys: ['5[c-f]'], count: 18 },
            { keys: ['6[0-5]'], count: 14 },
            { keys: ['6[6-9a]'], count: 18 },
            { keys: ['6[b-f]'], count: 20 },
            { keys: ['7[0-3]'], count: 17 },
            { keys: ['7[4-7]'], count: 20 },
            { keys: ['7[8-9a]'], count: 15 },
            { keys: ['7[b-d]'], count: 16 },
            { keys: ['7[e-f]'], count: 12 },
            { keys: ['8[0-3]'], count: 17 },
            { keys: ['8[4-6]'], count: 19 },
            { keys: ['8[7-8]'], count: 13 },
            { keys: ['8[9a]'], count: 11 },
            { keys: ['8[b-c]'], count: 5 },
            { keys: ['8[d-e]'], count: 6 },
            { keys: ['8[f]'], count: 4 },
            { keys: ['9[0-2]'], count: 13 },
            { keys: ['9[3-5]'], count: 16 },
            { keys: ['9[6-8]'], count: 11 },
            { keys: ['9[9a-b]'], count: 12 },
            { keys: ['9[c-e]'], count: 8 },
            { keys: ['9[f]'], count: 4 },
            { keys: ['a[0-4]'], count: 17 },
            { keys: ['a[5-8]'], count: 15 },
            { keys: ['a[9a-c]'], count: 17 },
            { keys: ['a[d-f]'], count: 9 },
            { keys: ['b[0-2]'], count: 7 },
            { keys: ['b[3-5]'], count: 15 },
            { keys: ['b[6-8]'], count: 18 },
            { keys: ['b[9a-b]'], count: 16 },
            { keys: ['b[c-e]'], count: 20 },
            { keys: ['b[f]'], count: 6 },
            { keys: ['c[0-3]'], count: 18 },
            { keys: ['c[4-5]'], count: 12 },
            { keys: ['c[6-7]'], count: 10 },
            { keys: ['c[8-9]'], count: 6 },
            { keys: ['c[a-b]'], count: 7 },
            { keys: ['c[c-d]'], count: 4 },
            { keys: ['c[e-f]'], count: 7 },
            { keys: ['d[0-5]'], count: 17 },
            { keys: ['d[6-9a-b]'], count: 19 },
            { keys: ['d[c-f]'], count: 13 },
            { keys: ['e[0-3]'], count: 18 },
            { keys: ['e[4-7]'], count: 15 },
            { keys: ['e[8-9a-b]'], count: 14 },
            { keys: ['e[c-f]'], count: 12 },
            { keys: ['f[0-5]'], count: 15 },
            { keys: ['f[6-9a]'], count: 18 },
            { keys: ['f[b-f]'], count: 18 },
        ];

        const totalSlicedCount = expectedResults.reduce((prev, curr) => {
            return curr.count + prev;
        }, 0);

        expect(results.length).toBeGreaterThan(1);

        results.forEach((result, index) => {
            expect(result).toMatchObject(expectedResults[index]);
        });

        expect(totalSlicedCount).toEqual(bulkData.length);
    });

    it('produces values starting at a specific depth', async () => {
        const opConfig = {
            key_range: ['a'],
            starting_key_depth: 3,
        };

        const test = await makeSlicerTest(opConfig);
        const results = await test.getAllSlices();

        const expectedResults = [
            { keys: ['aa5'], count: 1 },
            { keys: ['aa6'], count: 1 },
            { keys: ['aa7'], count: 1 },
            { keys: ['aba'], count: 1 },
            { keys: ['abc'], count: 1 },
            { keys: ['ab4'], count: 1 },
            { keys: ['ac3'], count: 1 },
            { keys: ['ac5'], count: 1 },
            { keys: ['ac8'], count: 1 },
            { keys: ['ad1'], count: 1 },
            { keys: ['ad4'], count: 1 },
            { keys: ['ad5'], count: 1 },
            { keys: ['ad6'], count: 1 },
            { keys: ['aea'], count: 1 },
            { keys: ['aef'], count: 1 },
            { keys: ['ae0'], count: 1 },
            { keys: ['ae8'], count: 1 },
            { keys: ['af4'], count: 1 },
            { keys: ['a0c'], count: 1 },
            { keys: ['a0f'], count: 2 },
            { keys: ['a01'], count: 1 },
            { keys: ['a04'], count: 1 },
            { keys: ['a1b'], count: 1 },
            { keys: ['a1c'], count: 1 },
            { keys: ['a1e'], count: 1 },
            { keys: ['a1f'], count: 1 },
            { keys: ['a11'], count: 2 },
            { keys: ['a17'], count: 1 },
            { keys: ['a36'], count: 2 },
            { keys: ['a4e'], count: 1 },
            { keys: ['a42'], count: 1 },
            { keys: ['a43'], count: 1 },
            { keys: ['a50'], count: 1 },
            { keys: ['a52'], count: 1 },
            { keys: ['a53'], count: 1 },
            { keys: ['a6d'], count: 1 },
            { keys: ['a6e'], count: 1 },
            { keys: ['a69'], count: 1 },
            { keys: ['a7a'], count: 1 },
            { keys: ['a7d'], count: 2 },
            { keys: ['a7e'], count: 1 },
            { keys: ['a8d'], count: 1 },
            { keys: ['a8f'], count: 2 },
            { keys: ['a89'], count: 2 },
            { keys: ['a9d'], count: 1 },
            { keys: ['a9e'], count: 2 },
            { keys: ['a94'], count: 2 },
            { keys: ['a95'], count: 1 },
            { keys: ['a97'], count: 2 },
        ];

        expect(results.pop()).toBeNull();

        results.forEach((result, index) => {
            expect(result).toMatchObject(expectedResults[index]);
        });
    });

    it('key range gets divided up by number of slicers', async () => {
        const opConfig = {
            key_range: ['a', 'b'],
        };

        const test = await makeSlicerTest(opConfig, 2);

        // @ts-expect-error
        const [slicer1, slicer2] = test.slicer()._slicers;

        await Promise.all([
            // @ts-expect-error
            test.slicer().processSlicer(slicer1),
            // @ts-expect-error
            test.slicer().processSlicer(slicer2),
        ]);

        const sliceResults = test.slicer().getSlices(1000);

        const slices1 = sliceResults.find((slice) => slice.slicer_id === 0);
        const slices2 = sliceResults.find((slice) => slice.slicer_id === 1);

        if (!slices1) throw new Error('slice1 was not found');
        if (!slices2) throw new Error('slice2 was not found');

        const expectedResults = [
            { keys: ['a'], count: 58 }, { keys: ['b'], count: 82 }
        ];

        expect(slices1.slicer_id).toEqual(0);
        expect(slices1.slicer_order).toEqual(1);
        expect(slices1.request).toMatchObject(expectedResults[0]);

        expect(slices2.slicer_id).toEqual(1);
        expect(slices2.slicer_order).toEqual(1);
        expect(slices2.request).toMatchObject(expectedResults[1]);

        const results = await test.createSlices();
        expect(results).toEqual([null, null]);
        // @ts-expect-error
        expect(test.slicer().isFinished).toBeTrue();
    });

    it('can return to previous position', async () => {
        const lastSlice = { keys: ['a6'], count: 3 };
        const retryData = [{ lastSlice, slicer_id: 0 }];
        const opConfig = { key_range: ['a'] };

        const test = await makeSlicerTest(opConfig, 1, retryData);
        const results = await test.getAllSlices();

        expect(results.pop()).toBeNull();

        const expectedResults = [
            { keys: ['a7'], count: 4 },
            { keys: ['a8'], count: 5 },
            { keys: ['a9'], count: 8 },
        ];

        results.forEach((result, index) => {
            expect(result).toMatchObject(expectedResults[index]);
        });
    });
});
