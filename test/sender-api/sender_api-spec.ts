import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import path from 'path';
import { RouteSenderAPI } from '@terascope/job-components';
import {
    TEST_INDEX_PREFIX, cleanupIndex, makeClient, fetch, waitForData
} from '../helpers';

describe('elasticsearch sender api', () => {
    const dir = __dirname;
    const assetDir = path.join(dir, '../../asset');

    const apiSendIndex = `${TEST_INDEX_PREFIX}_send_api_`;
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
        await cleanupIndex(esClient, `${apiSendIndex}*`);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, `${apiSendIndex}*`);
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    async function setupTest() {
        const job = newTestJobConfig({
            max_retries: 3,
            apis: [
                {
                    _name: 'elasticsearch_sender_api',
                    index: 'someIndex',
                },
            ],
            operations: [
                {
                    _op: 'test-reader',
                    passthrough_slice: true
                },
                {
                    _op: 'elasticsearch_index_selector',
                    index: apiSendIndex,
                    type: 'events'

                },
                {
                    _op: 'noop',
                    apiName: 'elasticsearch_sender_api'
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
            const api = processor.getAPI<RouteSenderAPI>(processor.opConfig.apiName);
            await api.send(data);
            return data;
        };

        await harness.initialize();

        return harness;
    }

    describe('elasticsearch_sender_api', () => {
        it('can send data to an index', async () => {
            const data = [{ some: 'data' }, { other: 'data' }];
            const test = await setupTest();
            const results = await test.runSlice(data);

            expect(results).toEqual(data);

            await waitForData(esClient, apiSendIndex, 2);

            const fetchResults = await fetch(esClient, { index: apiSendIndex, q: '*' });
            expect(fetchResults.length).toEqual(2);
        });
    });
});
