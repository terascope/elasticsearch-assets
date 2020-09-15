import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { getESVersion } from 'elasticsearch-store';
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
    const esClient = makeClient();

    const version = getESVersion(esClient);

    const docType = version === 5 ? 'events' : '_doc';

    const clients = [
        {
            type: 'elasticsearch',
            endpoint: 'default',
            create: () => ({
                client: esClient
            }),
        }
    ];

    let harness: WorkerTestHarness;

    beforeAll(async () => {
        await cleanupIndex(esClient, `${apiReaderIndex}*`);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, `${apiReaderIndex}*`);
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    async function setupAPITest(): Promise<ElasticReaderFactoryAPI> {
        const job = newTestJobConfig({
            max_retries: 3,
            apis: [
                {
                    _name: 'elasticsearch_reader_api',
                    index: apiReaderIndex,
                    type: docType
                },
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

        expect(api.version).toEqual(version);

        expect(apiFactory.size).toEqual(1);
    });

    it('can read data from an index', async () => {
        const data = [{ some: 'data' }, { other: 'data' }];

        await upload(esClient, { index: apiReaderIndex, type: docType }, data);

        await waitForData(esClient, apiReaderIndex, 2);

        const apiFactory = await setupAPITest();
        const api = await apiFactory.create('test', { query: '*' });

        // @ts-expect-error
        expect(api.windowSize).toBeUndefined();

        const results = await api.fetch({});
        expect(results.length).toEqual(2);

        // @ts-expect-error
        expect(api.windowSize).toEqual(10000);
    });

    it('can count data from an index', async () => {
        const index = `${apiReaderIndex}_count`;
        const data = [{ some: 'data' }, { other: 'data' }];

        await upload(esClient, { index, type: docType }, data);

        await waitForData(esClient, index, 2);

        const apiFactory = await setupAPITest();
        const api = await apiFactory.create('test', { query: '*' });

        const results = await api.count();
        expect(results).toEqual(2);
    });

    it('can check for index existence', async () => {
        const index = `${apiReaderIndex}_test`;
        const data = [{ some: 'data' }, { other: 'data' }];

        const apiFactory = await setupAPITest();
        const api = await apiFactory.create('test', { query: '*', index });

        await expect(api.verifyIndex()).toReject();

        await upload(esClient, { index, type: docType }, data);

        await waitForData(esClient, index, 2);

        await expect(api.verifyIndex()).toResolve();
    });
});
