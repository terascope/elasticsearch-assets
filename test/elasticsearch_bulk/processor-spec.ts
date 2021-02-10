import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { SearchParams, BulkIndexDocumentsParams } from 'elasticsearch';
import { getESVersion } from 'elasticsearch-store';
import { DataEntity, OpConfig } from '@terascope/job-components';
import {
    makeClient,
    cleanupIndex,
    fetch,
    upload,
    waitForData,
    TEST_INDEX_PREFIX,
} from '../helpers';

interface ClientCalls {
    [key: string]: BulkIndexDocumentsParams
}

describe('elasticsearch_bulk', () => {
    let harness: WorkerTestHarness;
    let clients: any;
    let clientCalls: ClientCalls = {};
    const esClient = makeClient();
    const bulkIndex = `${TEST_INDEX_PREFIX}_bulk_`;
    const version = getESVersion(esClient);

    const docType = version === 5 ? 'events' : '_doc';

    beforeAll(async () => {
        await cleanupIndex(esClient, `${bulkIndex}*`);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, `${bulkIndex}*`);
    });

    function proxyClient(endpoint: string) {
        const client = makeClient();
        const bulkFn = esClient.bulk.bind(client);
        client.bulk = (params: BulkIndexDocumentsParams) => {
            clientCalls[endpoint] = params;
            return bulkFn(params);
        };

        return client;
    }

    function makeIndex(index?: string) {
        if (index) return `${bulkIndex}-${index}`;
        return bulkIndex;
    }

    beforeEach(() => {
        clientCalls = {};
        clients = [
            {
                type: 'elasticsearch',
                endpoint: 'default',
                create: () => ({
                    client: proxyClient('default')
                }),
            },
            {
                type: 'elasticsearch',
                endpoint: 'otherConnection',
                create: () => ({
                    client: proxyClient('otherConnection')
                }),
            }
        ];
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    async function makeTest(opConfig = {}) {
        const bulkConfig: OpConfig = Object.assign(
            { _op: 'elasticsearch_bulk', index: bulkIndex },
            opConfig,
            { type: docType }
        );

        const job = newTestJobConfig({
            max_retries: 0,
            operations: [
                {
                    _op: 'test-reader',
                    passthrough_slice: true,
                },
                bulkConfig,
            ],
        });

        harness = new WorkerTestHarness(job, { clients });
        await harness.initialize();
        return harness;
    }

    it('if no docs, returns an empty array', async () => {
        const test = await makeTest();
        const results = await test.runSlice([]);

        expect(results).toEqual([]);
    });

    it('returns the data passed in', async () => {
        const testIndex = 'docs_returned';
        const index = makeIndex(testIndex);
        const data = [{ some: 'data' }, { other: 4 }];

        const test = await makeTest({ index });

        const results = await test.runSlice(data);

        expect(results).toEqual(data);
    });

    it('can send data to index', async () => {
        const testIndex = 'less_than_size';
        const index = makeIndex(testIndex);
        const opConfig = { size: 50, create: true, index };

        const query: SearchParams = {
            index,
            size: 200
        };
        const data = [];

        for (let i = 0; i < 50; i += 1) {
            data.push(DataEntity.make({ some: 'data' }, { _key: i + 1 }));
        }

        const test = await makeTest(opConfig);
        const results = await test.runSlice(data);

        expect(Array.isArray(results)).toEqual(true);
        expect(results).toEqual(data);

        await waitForData(esClient, index, data.length);

        const fetchedData = await fetch(esClient, query);

        expect(Array.isArray(fetchedData)).toEqual(true);
        expect(fetchedData).toEqual(data);
    });

    it('can send delete bulk calls that are over size', async () => {
        const testIndex = 'deleted_records';
        const index = makeIndex(testIndex);

        const opConfig = {
            size: 2,
            delete: true,
            index
        };
        const data = [{ some: 'data' }, { other: 'data' }, { final: 'data' }]
            .map((record, ind) => {
                const newData = DataEntity.make(record);
                newData.setKey(ind + 1);
                return newData;
            });

        const query: SearchParams = {
            index,
            size: 200
        };

        await upload(esClient, { index, type: '_doc' }, data);

        const test = await makeTest(opConfig);

        const reader = harness.getOperation('test-reader');
        // @ts-expect-error
        const fn = reader.fetch.bind(reader);
        // @ts-expect-error
        reader.fetch = async (_incDocs: DataEntity[]) => fn(data);
        const results = await test.runSlice(data);

        expect(Array.isArray(results)).toEqual(true);
        expect(results).toEqual(data);

        await waitForData(esClient, index, 0);

        const fetchedData = await fetch(esClient, query);

        expect(Array.isArray(fetchedData)).toEqual(true);
        expect(fetchedData).toEqual([]);
    });
});
