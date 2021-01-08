import 'jest-extended';
import { DataEntity } from '@terascope/job-components';
import { JobTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { getESVersion } from 'elasticsearch-store';
import { getKeyArray, IDType } from '@terascope/elasticsearch-asset-apis';
import {
    TEST_INDEX_PREFIX,
    ELASTICSEARCH_VERSION,
    getListOfIds,
    getTotalSliceCounts,
    makeClient,
    cleanupIndex,
    populateIndex
} from '../helpers';
import evenSpread from '../fixtures/data/even-spread';

describe('id_reader job', () => {
    let harness: JobTestHarness;
    let clients: any;
    const esClient = makeClient();
    const idIndex = `${TEST_INDEX_PREFIX}_id_job_`;

    const version = getESVersion(esClient);

    const docType = version === 5 ? 'events' : '_doc';
    // in es5 this should be ignored
    const id_field_name = 'uuid';
    const bulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    function makeIndex(str: string) {
        return `${idIndex}_${str}`;
    }

    const evenIndex = makeIndex(evenSpread.index);

    beforeAll(async () => {
        await cleanupIndex(esClient, makeIndex('*'));
        await populateIndex(esClient, evenIndex, evenSpread.types, bulkData, docType);
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
                config: {
                    apiVersion: ELASTICSEARCH_VERSION
                }
            },
            {
                type: 'elasticsearch',
                endpoint: 'otherConnection',
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

    it('can fetch all even-data with job in long form with deprecated "id_field_name"', async () => {
        const apiConfig = {
            _name: 'elasticsearch_reader_api',
            type: docType,
            field: id_field_name,
            index: evenIndex,
            key_type: IDType.base64url
        };

        const job = newTestJobConfig({
            slicers: 1,
            max_retries: 0,
            apis: [apiConfig],
            operations: [
                { _op: 'id_reader', api_name: 'elasticsearch_reader_api' },
                { _op: 'noop' }
            ],
        });

        harness = new JobTestHarness(job, { clients });

        await harness.initialize();

        const keyList = getKeyArray(IDType.base64url);

        const evenSpreadIds = getListOfIds(evenSpread.data, id_field_name);

        const sliceResults = await harness.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);

        sliceResults.forEach((results) => {
            const idChar = results.data[0][id_field_name].charAt(0);

            expect(keyList).toContain(idChar);
            expect(evenSpreadIds.has(idChar)).toEqual(true);
            expect(evenSpreadIds.get(idChar)).toEqual(results.data.length);
        });
    });

    it('can fetch all even-data with job in long form with non-deprecated', async () => {
        const apiConfig = {
            _name: 'elasticsearch_reader_api',
            type: docType,
            id_field_name,
            index: evenIndex,
            key_type: IDType.base64url
        };

        const job = newTestJobConfig({
            slicers: 1,
            max_retries: 0,
            apis: [apiConfig],
            operations: [
                { _op: 'id_reader', api_name: 'elasticsearch_reader_api' },
                { _op: 'noop' }
            ],
        });

        harness = new JobTestHarness(job, { clients });

        await harness.initialize();

        const keyList = getKeyArray(IDType.base64url);

        const evenSpreadIds = getListOfIds(evenSpread.data, id_field_name);

        const sliceResults = await harness.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);

        sliceResults.forEach((results) => {
            const idChar = results.data[0][id_field_name].charAt(0);

            expect(keyList).toContain(idChar);
            expect(evenSpreadIds.has(idChar)).toEqual(true);
            expect(evenSpreadIds.get(idChar)).toEqual(results.data.length);
        });
    });

    it('can fetch all even-data with job in short form', async () => {
        const job = newTestJobConfig({
            slicers: 1,
            max_retries: 0,
            apis: [],
            operations: [
                {
                    _op: 'id_reader',
                    type: docType,
                    id_field_name,
                    index: evenIndex,
                    key_type: IDType.base64url
                },
                { _op: 'noop' }
            ],
        });

        harness = new JobTestHarness(job, { clients });

        await harness.initialize();

        const keyList = getKeyArray(IDType.base64url);

        const evenSpreadIds = getListOfIds(evenSpread.data, id_field_name);

        const sliceResults = await harness.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);

        sliceResults.forEach((results) => {
            const idChar = results.data[0][id_field_name].charAt(0);

            expect(keyList).toContain(idChar);
            expect(evenSpreadIds.has(idChar)).toEqual(true);
            expect(evenSpreadIds.get(idChar)).toEqual(results.data.length);
        });
    });

    it('can fetch all even-data with job in long form but it makes its own api', async () => {
        const apiConfig = {
            _name: 'elasticsearch_reader_api',
            type: docType,
            id_field_name,
            index: 'something_else',
            key_type: IDType.base64url
        };
        // KEY DIFFERENCE IS LACK OF API_NAME, it will make 'elasticsearch_reader_api:id_reader-0'
        const job = newTestJobConfig({
            slicers: 1,
            max_retries: 0,
            apis: [apiConfig],
            operations: [
                {
                    _op: 'id_reader',
                    type: docType,
                    id_field_name,
                    index: evenIndex,
                    key_type: IDType.base64url
                },
                { _op: 'noop' }
            ],
        });

        harness = new JobTestHarness(job, { clients });

        await harness.initialize();

        const keyList = getKeyArray(IDType.base64url);

        const evenSpreadIds = getListOfIds(evenSpread.data, id_field_name);

        const sliceResults = await harness.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);

        sliceResults.forEach((results) => {
            const idChar = results.data[0][id_field_name].charAt(0);

            expect(keyList).toContain(idChar);
            expect(evenSpreadIds.has(idChar)).toEqual(true);
            expect(evenSpreadIds.get(idChar)).toEqual(results.data.length);
        });
    });
});
