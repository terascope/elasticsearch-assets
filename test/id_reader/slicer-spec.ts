import { SlicerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { SlicerRecoveryData, DataEntity, AnyObject } from '@terascope/job-components';
import { getESVersion } from 'elasticsearch-store';
import {
    TEST_INDEX_PREFIX,
    ELASTICSEARCH_VERSION,
    cleanupIndex,
    makeClient,
    populateIndex,
    waitForData
} from '../helpers';
import evenSpread from '../fixtures/id/even-spread';

describe('id_reader slicer', () => {
    jest.setTimeout(30 * 1000);

    const apiReaderIndex = `${TEST_INDEX_PREFIX}_id_slicer`;
    const esClient = makeClient();

    const version = getESVersion(esClient);
    const docType = version === 5 ? 'events' : '_doc';
    const field = 'uuid';

    // for compatibility tests for older elasticsearch version,
    // we make the _id of the record the same as its uuid field
    const bulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    let harness: SlicerTestHarness;
    let clients: any;

    interface ESData {
        count: number;
        key: string;
    }

    function formatExpectedData(data: ESData[]) {
        return data.map((obj) => {
            if (version <= 5) return { key: `${docType}#${obj.key}`, count: obj.count };
            return { wildcard: { field, value: obj.key }, count: obj.count };
        });
    }

    beforeAll(async () => {
        await cleanupIndex(esClient, `${apiReaderIndex}*`);
        await populateIndex(esClient, apiReaderIndex, evenSpread.types, bulkData, docType);
        await waitForData(esClient, apiReaderIndex, bulkData.length);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, `${apiReaderIndex}*`);
    });

    beforeEach(() => {
        clients = [
            {
                type: 'elasticsearch',
                endpoint: 'default',
                create: () => ({
                    client: esClient
                }),
                config: {
                    apiVersion: ELASTICSEARCH_VERSION
                }
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
        opConfig: AnyObject = {},
        numOfSlicers = 1,
        recoveryData?: SlicerRecoveryData[]
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

        const expectedResults = formatExpectedData([
            { key: 'a*', count: 58 },
            { key: 'b*', count: 82 },
            { key: 'c*', count: 64 },
            { key: 'd*', count: 49 },
            { key: 'e*', count: 59 },
            { key: 'f*', count: 51 },
            { key: '0*', count: 70 },
            { key: '1*', count: 55 },
            { key: '2*', count: 55 },
            { key: '3*', count: 54 },
            { key: '4*', count: 68 },
            { key: '5*', count: 64 },
            { key: '6*', count: 52 },
            { key: '7*', count: 80 },
            { key: '8*', count: 75 },
            { key: '9*', count: 64 },
        ]);

        results.forEach((result, index) => {
            expect(result).toMatchObject(expectedResults[index]);
        });
    });

    it('can call on a subset of keys', async () => {
        const test = await makeSlicerTest({ key_range: ['a', 'b'] });
        const results = await test.getAllSlices();

        // null shows we are done, we remove to compare objects
        expect(results.pop()).toBeNull();

        const expectedResults = formatExpectedData([
            { key: 'a*', count: 58 },
            { key: 'b*', count: 82 },
        ]);

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

        const expectedResults = formatExpectedData([
            { key: 'a0*', count: 5 },
            { key: 'a1*', count: 7 },
            { key: 'a3*', count: 2 },
            { key: 'a4*', count: 3 },
            { key: 'a5*', count: 3 },
            { key: 'a6*', count: 3 },
            { key: 'a7*', count: 4 },
            { key: 'a8*', count: 5 },
            { key: 'a9*', count: 8 },
            { key: 'aa*', count: 3 },
            { key: 'ab*', count: 3 },
            { key: 'ac*', count: 3 },
            { key: 'ad*', count: 4 },
            { key: 'ae*', count: 4 },
            { key: 'af*', count: 1 },
        ]);

        results.forEach((result, index) => {
            expect(result).toMatchObject(expectedResults[index]);
        });
    });

    it('produces values starting at a specific depth', async () => {
        const opConfig = {
            key_range: ['a'],
            starting_key_depth: 3,
        };

        const test = await makeSlicerTest(opConfig);
        const results = await test.getAllSlices();

        const expectedResults = formatExpectedData([
            { key: 'aa5*', count: 1 },
            { key: 'aa6*', count: 1 },
            { key: 'aa7*', count: 1 },
            { key: 'aba*', count: 1 },
            { key: 'abc*', count: 1 },
            { key: 'ab4*', count: 1 },
            { key: 'ac3*', count: 1 },
            { key: 'ac5*', count: 1 },
            { key: 'ac8*', count: 1 },
            { key: 'ad1*', count: 1 },
            { key: 'ad4*', count: 1 },
            { key: 'ad5*', count: 1 },
            { key: 'ad6*', count: 1 },
            { key: 'aea*', count: 1 },
            { key: 'aef*', count: 1 },
            { key: 'ae0*', count: 1 },
            { key: 'ae8*', count: 1 },
            { key: 'af4*', count: 1 },
            { key: 'a0c*', count: 1 },
            { key: 'a0f*', count: 2 },
            { key: 'a01*', count: 1 },
            { key: 'a04*', count: 1 },
            { key: 'a1b*', count: 1 },
            { key: 'a1c*', count: 1 },
            { key: 'a1e*', count: 1 },
            { key: 'a1f*', count: 1 },
            { key: 'a11*', count: 2 },
            { key: 'a17*', count: 1 },
            { key: 'a36*', count: 2 },
            { key: 'a4e*', count: 1 },
            { key: 'a42*', count: 1 },
            { key: 'a43*', count: 1 },
            { key: 'a50*', count: 1 },
            { key: 'a52*', count: 1 },
            { key: 'a53*', count: 1 },
            { key: 'a6d*', count: 1 },
            { key: 'a6e*', count: 1 },
            { key: 'a69*', count: 1 },
            { key: 'a7a*', count: 1 },
            { key: 'a7d*', count: 2 },
            { key: 'a7e*', count: 1 },
            { key: 'a8d*', count: 1 },
            { key: 'a8f*', count: 2 },
            { key: 'a89*', count: 2 },
            { key: 'a9d*', count: 1 },
            { key: 'a9e*', count: 2 },
            { key: 'a94*', count: 2 },
            { key: 'a95*', count: 1 },
            { key: 'a97*', count: 2 },
        ]);

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

        const [slices1, slices2] = test.slicer().getSlices(1000);

        const expectedResults = formatExpectedData([{ key: 'a*', count: 58 }, { key: 'b*', count: 82 }]);

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
        const lastSlice = formatExpectedData([{ key: 'a6*', count: 3 }])[0];
        const retryData = [{ lastSlice, slicer_id: 0 }];
        const opConfig = { key_range: ['a'] };

        const test = await makeSlicerTest(opConfig, 1, retryData);
        const results = await test.getAllSlices();

        expect(results.pop()).toBeNull();

        const expectedResults = formatExpectedData([
            { key: 'a7*', count: 4 },
            { key: 'a8*', count: 5 },
            { key: 'a9*', count: 8 },
        ]);

        results.forEach((result, index) => {
            expect(result).toMatchObject(expectedResults[index]);
        });
    });
});
