import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { ClientParams } from '@terascope/types';
import { OpConfig, TestClientConfig } from '@terascope/job-components';
import { DataEntity, debugLogger } from '@terascope/core-utils';
import {
    makeClient, cleanupIndex, fetch,
    upload, waitForData, TEST_INDEX_PREFIX,
} from '../helpers/index.js';

interface ClientCalls {
    [key: string]: ClientParams.BulkParams;
}

describe('elasticsearch_bulk', () => {
    const bulkIndex = `${TEST_INDEX_PREFIX}_bulk_`;
    const logger = debugLogger('test-logger');

    let harness: WorkerTestHarness;
    let clients: TestClientConfig[];
    let clientCalls: ClientCalls = {};
    let esClient: any;

    beforeAll(async () => {
        esClient = await makeClient();
        await cleanupIndex(esClient, `${bulkIndex}*`);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, `${bulkIndex}*`);
    });

    async function proxyClient(endpoint: string) {
        const client = await makeClient();
        const bulkFn = esClient.bulk.bind(client);

        client.bulk = (params: ClientParams.BulkParams) => {
            clientCalls[endpoint] = params;
            return bulkFn(params);
        };

        return client;
    }

    function makeIndex(index?: string) {
        if (index) return `${bulkIndex}-${index}`;
        return bulkIndex;
    }

    beforeEach(async () => {
        const [defaultClient, otherClient] = await Promise.all([
            proxyClient('default'),
            proxyClient('otherConnection')
        ]);

        clientCalls = {};
        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: defaultClient,
                    logger
                }),
            },
            {
                type: 'elasticsearch-next',
                endpoint: 'otherConnection',
                createClient: async () => ({
                    client: otherClient,
                    logger
                }),
            }
        ];
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    async function makeTest(opConfig = {}) {
        const bulkConfig: OpConfig = Object.assign(
            { _op: 'elasticsearch_bulk' },
            opConfig,
        );

        const job = newTestJobConfig({
            max_retries: 0,
            apis: [{ _name: 'elasticsearch_sender_api', index: bulkIndex }],
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

        const query: ClientParams.SearchParams = {
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

        const query: ClientParams.SearchParams = {
            index,
            size: 200
        };

        await upload(esClient, { index }, data);

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

    it('should send docs to kafka dead letter queue if _dead_letter_action is kafka_dead_letter', async () => {
        const index = `${bulkIndex}-dlq-test`;

        // adjust metadata to simulate a bulk rejection
        const data = [
            { _key: 1, test_field: 2 },
            { _key: 2, test_field: 4 }
        ].map((doc) => DataEntity.make(doc, { _key: doc._key, _bulk_sender_rejection: 'unretryable error' }));

        const test = await makeTest({
            _op: 'elasticsearch_bulk',
            index,
            _dead_letter_action: 'none'
        });

        const esBulk = test.getOperation('elasticsearch_bulk');

        const rejected: (DataEntity | Error)[] = [];

        // replace reject record function to verify the doc and err is getting passed in
        esBulk.rejectRecord = (doc: DataEntity, err: Error) => {
            rejected.push(doc, err);
            return null;
        };

        // replace opConfig _dead_letter_action setting to trigger reject record logic
        Object.defineProperty(
            esBulk,
            'opConfig',
            {
                value: { _dead_letter_action: 'kafka_dead_letter' },
                writable: false
            }
        );

        await test.runSlice(data);

        expect(rejected).toEqual([
            DataEntity.make({ _key: 1, test_field: 2 }),
            'unretryable error',
            DataEntity.make({ _key: 2, test_field: 4 }),
            'unretryable error'
        ]);
    });
});
