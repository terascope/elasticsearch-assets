import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import path from 'path';
import elasticApi from '@terascope/elasticsearch-api';
import {
    TEST_INDEX_PREFIX, cleanupIndex, makeClient, upload, waitForData
} from '../helpers';

describe('elasticsearch reader api', () => {
    const dir = __dirname;
    const assetDir = path.join(dir, '../../asset');

    const apiReaderIndex = `${TEST_INDEX_PREFIX}_reader_api_`;
    const esClient = makeClient();

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

    async function setupTest() {
        const job = newTestJobConfig({
            max_retries: 3,
            apis: [
                {
                    _name: 'elasticsearch_reader_api',
                    index: apiReaderIndex,
                },
            ],
            operations: [
                {
                    _op: 'test-reader',
                    passthrough_slice: true
                },
                {
                    _op: 'noop',
                    apiName: 'elasticsearch_reader_api'
                }
            ],
        });

        harness = new WorkerTestHarness(job, {
            assetDir,
            clients
        });

        const processor = harness.getOperation('noop');
        // @ts-expect-error\
        processor.onBatch = async function test(data: DataEntity[]) {
            const api = processor.getAPI<elasticApi.Client>(processor.opConfig.apiName);
            return api.search(data[0]);
        };

        await harness.initialize();

        return harness;
    }

    it('can read data from an index', async () => {
        const data = [{ some: 'data' }, { other: 'data' }];

        await upload(esClient, { index: apiReaderIndex, type: 'events' }, data);

        await waitForData(esClient, apiReaderIndex, 2);

        const slice = [{ index: apiReaderIndex, q: '*' }];
        const test = await setupTest();
        const results = await test.runSlice(slice);

        expect(results.length).toEqual(2);
    });
});
