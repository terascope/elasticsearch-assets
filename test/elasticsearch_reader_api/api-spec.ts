import 'jest-extended';
import { DataEntity } from '@terascope/job-components';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { ElasticReaderFactoryAPI } from '../../asset/src/elasticsearch_reader_api/interfaces';
import {
    TEST_INDEX_PREFIX,
    cleanupIndex,
    makeClient,
    upload,
    waitForData
} from '../helpers';

describe('elasticsearch reader api', () => {
    const apiReaderIndex = `${TEST_INDEX_PREFIX}_reader_api_`;

    const docType = '_doc';

    let harness: WorkerTestHarness;
    let esClient: any;
    let clients: any;

    beforeAll(async () => {
        esClient = await makeClient();

        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: esClient
                }),
            }
        ];

        await cleanupIndex(esClient, `${apiReaderIndex}*`);

        const data = [{ some: 'data' }, { other: 'data' }];

        await upload(esClient, { index: apiReaderIndex, type: docType }, data);

        await waitForData(esClient, apiReaderIndex, 2);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, `${apiReaderIndex}*`);
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    async function setupAPITest(
        overrideConfig: Record<string, any> = {}
    ): Promise<ElasticReaderFactoryAPI> {
        const config = Object.assign(
            {},
            {
                _name: 'elasticsearch_reader_api',
                index: apiReaderIndex,
                type: docType
            },
            overrideConfig
        );

        const job = newTestJobConfig({
            max_retries: 3,
            apis: [
                config
            ],
            operations: [
                {
                    _op: 'test-reader',
                    passthrough_slice: true
                },
                {
                    _op: 'noop'
                }
            ],
        });

        harness = new WorkerTestHarness(job, { clients });

        await harness.initialize();

        return harness.getAPI('elasticsearch_reader_api');
    }

    it('has APIFactory methods, can return a reader', async () => {
        const apiFactory = await setupAPITest();

        expect(apiFactory.size).toEqual(0);

        const api = await apiFactory.create('test', {});

        expect(api.config.index).toEqual(apiReaderIndex);
        expect(api.fetch).toBeFunction();
        expect(api.count).toBeFunction();

        expect(apiFactory.size).toEqual(1);
    });

    it('can read data from an index', async () => {
        const apiFactory = await setupAPITest();
        const api = await apiFactory.create('test', { query: '*' });

        const results = await api.fetch({}) as unknown as DataEntity[];
        expect(results.length).toEqual(2);

        expect(api.windowSize).toEqual(10000);
    });

    it('can count data from an index', async () => {
        const apiFactory = await setupAPITest();
        const api = await apiFactory.create('test', { query: '*' });

        const results = await api.count();
        expect(results).toEqual(2);
    });

    it('can check for index existence', async () => {
        const apiFactory = await setupAPITest();
        const api = await apiFactory.create('test', { query: '*' });

        await expect(api.verifyIndex()).toResolve();
    });

    it('should throw if in size is greater than window', async () => {
        const size = 1000000000;
        const errMsg = `Invalid parameter size: ${size}, it cannot exceed the "index.max_result_window" index setting of 10000 for index ${apiReaderIndex}`;
        const apiFactory = await setupAPITest({ size });

        try {
            const api = await apiFactory.create('test', { query: '*' });
            await api.fetch({});
            throw new Error('expected error here');
        } catch (err) {
            expect(
                // @ts-expect-error
                err.message
            ).toEqual(errMsg);
        }
    });
});
