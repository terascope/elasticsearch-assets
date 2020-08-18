import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { DataEntity } from '@terascope/job-components';
import { getESVersion } from 'elasticsearch-store';
import { ESCachedStateStorage } from '@terascope/teraslice-state-storage';
import {
    TEST_INDEX_PREFIX,
    cleanupIndex,
    makeClient,
    waitForData,
    fetch
} from '../helpers';

describe('elasticsearch state storage api', () => {
    const idField = '_key';
    const apiReaderIndex = `${TEST_INDEX_PREFIX}_state__storage_api_`;
    const esClient = makeClient();
    const version = getESVersion(esClient);
    const docType = version === 5 ? 'type' : '_doc';
    let api: ESCachedStateStorage;
    const clients = [
        {
            type: 'elasticsearch',
            endpoint: 'default',
            create: () => ({
                client: esClient
            }),
        }
    ];

    function addTestMeta(obj: any, index: number) {
        return DataEntity.make(obj, { [idField]: index + 1 });
    }

    const docArray = [
        {
            data: 'thisIsFirstData'
        },
        {
            data: 'thisIsSecondData'
        },
        {
            data: 'thisIsThirdData'
        }
    ].map(addTestMeta);

    const apiName = 'elasticsearch_state_storage';

    const job = newTestJobConfig({
        max_retries: 3,
        apis: [
            {
                _name: apiName,
                index: apiReaderIndex,
                cache_size: 100000,
                type: docType,
                persist: true
            }
        ],
        operations: [
            {
                _op: 'test-reader',
                passthrough_slice: true
            },
            {
                _op: 'noop',
            },

        ],
    });

    let harness: WorkerTestHarness;

    beforeAll(async () => {
        await cleanupIndex(esClient, `${apiReaderIndex}*`);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, `${apiReaderIndex}*`);
    });

    beforeEach(async () => {
        harness = new WorkerTestHarness(job, { clients });

        await harness.initialize();

        api = harness.getAPI(apiName);
    });

    afterEach(async () => {
        await harness.shutdown();
    });

    it('can run and use the api', async () => {
        expect.hasAssertions();

        docArray.forEach((record) => {
            expect(api.isCached(record)).toBeFalse();
        });

        await api.mset(docArray);

        await waitForData(esClient, apiReaderIndex, 3);

        const results = await fetch(esClient, { index: apiReaderIndex, size: 1000, q: '*' });

        expect(results).toBeArrayOfSize(3);

        docArray.forEach((record) => {
            expect(api.isCached(record)).toBeTrue();
        });
    });
});
