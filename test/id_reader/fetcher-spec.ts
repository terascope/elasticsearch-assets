import 'jest-extended';
import { DataEntity } from '@terascope/job-components';
import { JobTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { getKeyArray, IDType } from '@terascope/elasticsearch-asset-apis';
import {
    TEST_INDEX_PREFIX,
    getListOfIds,
    getTotalSliceCounts,
    makeClient,
    cleanupIndex,
    populateIndex
} from '../helpers';
import evenSpread from '../fixtures/data/even-spread';

describe('id_reader fetcher', () => {
    const idIndex = `${TEST_INDEX_PREFIX}_id_fetcher_`;

    const docType = '_doc';
    // in es5 this should be ignored
    const id_field_name = 'uuid';
    const bulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    function makeIndex(str: string) {
        return `${idIndex}_${str}`;
    }

    const evenIndex = makeIndex(evenSpread.index);

    let harness: JobTestHarness;
    let clients: any;
    let esClient: any;

    beforeAll(async () => {
        esClient = await makeClient();

        await cleanupIndex(esClient, makeIndex('*'));
        await populateIndex(esClient, evenIndex, evenSpread.types, bulkData, docType);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, makeIndex('*'));
    });

    beforeEach(() => {
        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: esClient
                })
            },
            {
                type: 'elasticsearch-next',
                endpoint: 'otherConnection',
                createClient: async () => ({
                    client: esClient
                })
            }
        ];
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    async function makeTest(opConfig?: any, numOfSlicers = 1) {
        const idReader = Object.assign(
            { _op: 'id_reader' },
            opConfig,
            { type: docType, id_field_name }
        );

        const job = newTestJobConfig({
            slicers: numOfSlicers,
            max_retries: 0,
            operations: [
                idReader,
                { _op: 'noop' }
            ],
        });

        harness = new JobTestHarness(job, { clients });

        await harness.initialize();

        return harness;
    }

    it('can fetch all even-data', async () => {
        const opConfig = { index: evenIndex };
        const keyList = getKeyArray(IDType.base64url);
        const test = await makeTest(opConfig);
        const evenSpreadIds = getListOfIds(evenSpread.data, id_field_name);

        const sliceResults = await test.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);

        sliceResults.forEach((results) => {
            const idChar = results.data[0][id_field_name].charAt(0);

            expect(keyList).toContain(idChar);
            expect(evenSpreadIds.has(idChar)).toEqual(true);
            expect(evenSpreadIds.get(idChar)).toEqual(results.data.length);
        });
    });

    it('can fetch all even-data with multiple slicers', async () => {
        const opConfig = { index: evenIndex };
        const keyList = getKeyArray(IDType.base64url);
        const test = await makeTest(opConfig, 2);
        const evenSpreadIds = getListOfIds(evenSpread.data, id_field_name);

        const sliceResults = await test.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);

        sliceResults.forEach((results) => {
            const idChar = results.data[0][id_field_name].charAt(0);

            expect(keyList).toContain(idChar);
            expect(evenSpreadIds.has(idChar)).toEqual(true);
            expect(evenSpreadIds.get(idChar)).toEqual(results.data.length);
        });
    });

    it('can fetch all even-data for a given key', async () => {
        const opConfig = {
            index: evenIndex,
            key_range: ['a']
        };
        const keyList = getKeyArray(IDType.base64url);
        const test = await makeTest(opConfig);
        const evenSpreadIds = getListOfIds(evenSpread.data, id_field_name);

        const sliceResults = await test.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(evenSpreadIds.get('a'));

        sliceResults.forEach((results) => {
            const idChar = results.data[0][id_field_name].charAt(0);

            expect(keyList).toContain(idChar);
            expect(evenSpreadIds.has(idChar)).toEqual(true);
            expect(evenSpreadIds.get(idChar)).toEqual(results.data.length);
        });
    });

    it('will have all appropriate metadata on records', async () => {
        const opConfig = {
            index: evenIndex,
            key_range: ['a']
        };

        const test = await makeTest(opConfig);
        const results = await test.runToCompletion();
        const record = results[0].data[0];

        expect(DataEntity.isDataEntity(record)).toBeTrue();

        const metadata = record.getMetadata();

        expect(metadata._createTime).toBeNumber();
        expect(metadata._processTime).toBeNumber();
        expect(metadata._ingestTime).toBeNumber();
        expect(metadata._eventTime).toBeNumber();
        expect(metadata._key).toBeString();
        expect(metadata._index).toEqual(evenIndex);
        expect(metadata._type).toEqual(docType);
        expect(metadata._eventTime).toBeNumber();
    });
});
