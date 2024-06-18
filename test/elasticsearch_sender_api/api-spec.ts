import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { isNil, debugLogger, TestClientConfig } from '@terascope/job-components';
import {
    TEST_INDEX_PREFIX, cleanupIndex, makeClient,
    fetch, waitForData
} from '../helpers/index.js';
import { ElasticSenderAPI } from '../../asset/src/elasticsearch_sender_api/interfaces.js';

describe('elasticsearch sender api', () => {
    const apiSendIndex = `${TEST_INDEX_PREFIX}_send_api_`;
    const docType = '_doc';
    const logger = debugLogger('test-logger');

    let harness: WorkerTestHarness;
    let esClient: any;
    let clients: TestClientConfig[];

    beforeAll(async () => {
        esClient = await makeClient();
        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: esClient,
                    logger

                }),
            }
        ];
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
                    index: apiSendIndex,
                    type: docType
                },
            ],
            operations: [
                {
                    _op: 'test-reader',
                    passthrough_slice: true
                },
                {
                    _op: 'noop',
                    apiName: 'elasticsearch_sender_api'
                }
            ],
        });

        harness = new WorkerTestHarness(job, { clients });

        const processor = harness.getOperation('noop');
        // @ts-expect-error\
        processor.onBatch = async function test(data: DataEntity[]) {
            const { apiName } = processor.opConfig;
            const apiManager = processor.getAPI<ElasticSenderAPI>(apiName);
            let api = apiManager.get(apiName);

            if (isNil(api)) api = await apiManager.create(apiName, this.opConfig);
            await api.send(data);
            return data;
        };

        await harness.initialize();

        return harness;
    }

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
