import {
    JobTestHarness, newTestJobConfig, SlicerTestHarness
} from 'teraslice-test-harness';
import path from 'path';
import { getKeyArray } from '../asset/src/id_reader/helpers';
import {
    makeClient, cleanupIndex, populateIndex
} from './helpers/elasticsearch';
import { TEST_INDEX_PREFIX, getListOfIds, getTotalSliceCounts } from './helpers';
import evenSpread from './fixtures/id/even-spread';

describe('id_reader', () => {
    const assetDir = path.join(__dirname, '..');
    let harness: JobTestHarness;
    let slicerHarness: SlicerTestHarness;
    let clients: any;
    const esClient = makeClient();
    const idIndex = `${TEST_INDEX_PREFIX}_id_`;

    function makeIndex(str: string) {
        return `${idIndex}_${str}`;
    }

    const evenIndex = makeIndex(evenSpread.index);

    beforeAll(async () => {
        await cleanupIndex(esClient, makeIndex('*'));
        await populateIndex(esClient, evenIndex, evenSpread.types, evenSpread.data);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, makeIndex('*'));
    });

    beforeEach(() => {
        clients = [
            {
                type: 'elasticsearch',
                endpoint: 'default',
                create: () => ({
                    client: esClient
                }),
            },
            {
                type: 'elasticsearch',
                endpoint: 'otherConnection',
                create: () => ({
                    client: esClient
                }),
            }
        ];
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
        if (slicerHarness) await slicerHarness.shutdown();
    });

    async function makeTest(opConfig?: any, numOfSlicers = 1) {
        const idReader = Object.assign({
            _op: 'id_reader',
        }, opConfig);
        const job = newTestJobConfig({
            slicers: numOfSlicers,
            max_retries: 0,
            operations: [
                idReader,
                { _op: 'noop' }
            ],
        });

        harness = new JobTestHarness(job, { assetDir, clients });
        slicerHarness = new SlicerTestHarness(job, { assetDir, clients });

        await harness.initialize();
        await slicerHarness.initialize();
        return harness;
    }

    it('can fetch all even-data', async () => {
        const field = 'uuid';

        const opConfig = {
            index: evenIndex,
            field
        };
        const keyList = getKeyArray({ key_type: 'base64url' } as any);
        const test = await makeTest(opConfig);
        const evenSpreadIds = getListOfIds(evenSpread.data, field);

        const sliceResults = await test.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);

        sliceResults.forEach((results) => {
            const idChar = results.data[0][field].charAt(0);

            expect(keyList).toContain(idChar);
            expect(evenSpreadIds.has(idChar)).toEqual(true);
            expect(evenSpreadIds.get(idChar)).toEqual(results.data.length);
        });
    });

    it('can fetch all even-data with multiple slicers', async () => {
        const field = 'uuid';

        const opConfig = {
            index: evenIndex,
            field
        };
        const keyList = getKeyArray({ key_type: 'base64url' } as any);
        const test = await makeTest(opConfig, 2);
        const evenSpreadIds = getListOfIds(evenSpread.data, field);

        const sliceResults = await test.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);

        sliceResults.forEach((results) => {
            const idChar = results.data[0][field].charAt(0);

            expect(keyList).toContain(idChar);
            expect(evenSpreadIds.has(idChar)).toEqual(true);
            expect(evenSpreadIds.get(idChar)).toEqual(results.data.length);
        });
    });

    it('can fetch all even-data for a given key', async () => {
        const field = 'uuid';

        const opConfig = {
            index: evenIndex,
            field,
            key_range: ['a']
        };
        const keyList = getKeyArray({ key_type: 'base64url' } as any);
        const test = await makeTest(opConfig);
        const evenSpreadIds = getListOfIds(evenSpread.data, field);

        const sliceResults = await test.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(evenSpreadIds.get('a'));

        sliceResults.forEach((results) => {
            const idChar = results.data[0][field].charAt(0);

            expect(keyList).toContain(idChar);
            expect(evenSpreadIds.has(idChar)).toEqual(true);
            expect(evenSpreadIds.get(idChar)).toEqual(results.data.length);
        });
    });
});
