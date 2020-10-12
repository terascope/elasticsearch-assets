import 'jest-extended';
import { DataEntity } from '@terascope/job-components';
import { JobTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { getESVersion } from 'elasticsearch-store';
import {
    TEST_INDEX_PREFIX,
    ELASTICSEARCH_VERSION,
    getTotalSliceCounts,
    makeClient,
    cleanupIndex,
    populateIndex
} from '../helpers';
import evenSpread from '../fixtures/data/even-spread';

describe('date_reader job', () => {
    const esClient = makeClient();
    const idIndex = `${TEST_INDEX_PREFIX}_es_reader_job_`;
    const date_field_name = 'created';
    const version = getESVersion(esClient);

    let harness: JobTestHarness;
    let clients: any;
    const docType = version === 5 ? 'events' : '_doc';
    // in es5 this should be ignored
    const bulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    function makeIndex(str: string) {
        return `${idIndex}_${str}`;
    }

    const evenIndex = makeIndex(`${evenSpread.index}-2020.0.1`);

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
                    index: `${makeIndex(evenSpread.index)}-*`,
                    type: '_doc',
                    date_field_name: 'created',
                    interval: 'auto',
                    time_resolution: 'ms',
                    size: 100000
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
