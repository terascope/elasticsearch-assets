import 'jest-extended';
import { ElasticsearchTestHelpers } from 'elasticsearch-store';
import { JobTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import {
    TEST_INDEX_PREFIX,
    getTotalSliceCounts,
    makeClient,
    cleanupIndex,
    populateIndex
} from '../helpers/index.js';

describe('date_reader job', () => {
    const idIndex = `${TEST_INDEX_PREFIX}_es_reader_job_`;
    const date_field_name = 'created';
    const evenSpread = ElasticsearchTestHelpers.EvenDateData;
    const docType = '_doc';
    const bulkData = evenSpread.data;

    function makeIndex(str: string) {
        return `${idIndex}_${str}`;
    }

    const evenIndex = makeIndex('even_spread-2020.0.1');

    let harness: JobTestHarness;
    let clients: any;
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

    it('can fetch all even-data with job in long form', async () => {
        const apiConfig = {
            _name: 'elasticsearch_reader_api',
            type: docType,
            index: evenIndex,
            date_field_name
        };

        const job = newTestJobConfig({
            slicers: 1,
            max_retries: 0,
            apis: [apiConfig],
            operations: [
                { _op: 'elasticsearch_reader', api_name: 'elasticsearch_reader_api' },
                { _op: 'noop' }
            ],
        });

        harness = new JobTestHarness(job, { clients });

        await harness.initialize();

        const sliceResults = await harness.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);
    });

    it('can fetch all even-data with job in short form', async () => {
        const job = newTestJobConfig({
            slicers: 1,
            max_retries: 0,
            apis: [],
            operations: [
                {
                    _op: 'elasticsearch_reader',
                    type: docType,
                    index: evenIndex,
                    date_field_name
                },
                { _op: 'noop' }
            ],
        });

        harness = new JobTestHarness(job, { clients });

        await harness.initialize();

        const sliceResults = await harness.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);
    });

    it('can fetch all even-data with job in long form but it makes its own api', async () => {
        const apiConfig = {
            _name: 'elasticsearch_reader_api',
            type: docType,
            index: 'something_else',
        };

        // KEY DIFFERENCE IS LACK OF API_NAME,
        // it will make 'elasticsearch_reader_api:elasticsearch_reader-0'
        const job = newTestJobConfig({
            slicers: 1,
            max_retries: 0,
            apis: [apiConfig],
            operations: [
                {
                    _op: 'elasticsearch_reader',
                    type: docType,
                    index: evenIndex,
                    date_field_name
                },
                { _op: 'noop' }
            ],
        });

        harness = new JobTestHarness(job, { clients });

        await harness.initialize();

        const sliceResults = await harness.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);
    });

    it('can read indicies with a "*"', async () => {
        const job = newTestJobConfig({
            name: 'test_job',
            lifecycle: 'once',
            workers: 1,
            apis: [
                {
                    _name: 'elasticsearch_reader_api',
                    connection: 'otherConnection',
                    index: `${makeIndex('even_spread')}-*`,
                    type: '_doc',
                    date_field_name: 'created',
                    interval: 'auto',
                    time_resolution: 'ms',
                    size: 10000
                }
            ],
            operations: [
                {
                    _op: 'elasticsearch_reader',
                    api_name: 'elasticsearch_reader_api'
                },
                { _op: 'noop' }
            ]
        });

        harness = new JobTestHarness(job, { clients });

        await harness.initialize();

        const sliceResults = await harness.runToCompletion();

        expect(getTotalSliceCounts(sliceResults)).toEqual(1000);
    });
});
