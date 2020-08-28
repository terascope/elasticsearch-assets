import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { APIFactoryRegistry, AnyObject } from '@terascope/job-components';
import { getESVersion } from 'elasticsearch-store';
import {
    TEST_INDEX_PREFIX,
    cleanupIndex,
    makeClient,
    upload,
    waitForData
} from '../helpers';
import Reader from '../../asset/src/elasticsearch_reader_api/reader';

describe('elasticsearch reader api', () => {
    const apiReaderIndex = `${TEST_INDEX_PREFIX}_reader_api_`;
    const esClient = makeClient();

    const version = getESVersion(esClient);

    const docType = version === 5 ? 'events' : '_doc';

    type API = APIFactoryRegistry<Reader, AnyObject>

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

    async function setupAPITest() {
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
                    _op: 'noop',
                    apiName: 'elasticsearch_reader_api'
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

    fit('can read data from an index', async () => {
        const data = [{ some: 'data' }, { other: 'data' }];

        await upload(esClient, { index: apiReaderIndex, type: docType }, data);

        await waitForData(esClient, apiReaderIndex, 2);

        const apiFactory = await setupAPITest();
        const api = await apiFactory.create('test', { query: '*' });

        const results = await api.fetch({});
        expect(results.length).toEqual(2);
    });
});
