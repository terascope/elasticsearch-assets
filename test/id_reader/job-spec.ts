import 'jest-extended';
import { ElasticsearchTestHelpers } from '@terascope/opensearch-client';
import { TestClientConfig, debugLogger } from '@terascope/job-components';
import { JobTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { getKeyArray, IDType } from '@terascope/elasticsearch-asset-apis';
import {
    TEST_INDEX_PREFIX,
    getListOfIds,
    getTotalSliceCounts,
    makeClient,
    cleanupIndex,
    populateIndex
} from '../helpers/index.js';

describe('id_reader job', () => {
    const idIndex = `${TEST_INDEX_PREFIX}_id_job_`;
    const docType = '_doc';
    const logger = debugLogger('test-logger');
    // in es5 this should be ignored
    const id_field_name = 'uuid';
    const evenSpread = ElasticsearchTestHelpers.EvenDateData;

    const bulkData = evenSpread.data;

    function makeIndex(str: string) {
        return `${idIndex}_${str}`;
    }

    const evenIndex = makeIndex('even_data');

    let harness: JobTestHarness;
    let clients: TestClientConfig[];
    let esClient: any;

    beforeAll(async () => {
        esClient = await makeClient();
        await cleanupIndex(esClient, makeIndex('*'));
        await populateIndex(esClient, evenIndex, evenSpread.EvenDataType, bulkData, docType);
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
                    client: esClient,
                    logger
                })
            },
            {
                type: 'elasticsearch-next',
                endpoint: 'otherConnection',
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

    it('can fetch all even-data with recursive optimizations set to true', async () => {
        const apiConfig = {
            _name: 'elasticsearch_reader_api',
            type: docType,
            field: id_field_name,
            index: evenIndex,
            key_type: IDType.base64url,
            recurse_optimization: true,
            size: 40
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

        const sliceResults = await harness.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);
    });
});
