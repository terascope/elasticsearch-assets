/* eslint-disable @typescript-eslint/no-unused-vars */
import { WorkerTestHarness, newTestJobConfig, JobHarnessOptions } from 'teraslice-test-harness';
import { SearchParams } from 'elasticsearch';
import {
    cloneDeep, isPlainObject, DataEntity, pDelay, __ENTITY_METADATA_KEY
} from '@terascope/job-components';
import path from 'path';
import {
    makeClient, cleanupIndex, fetch, upload
} from './helpers/elasticsearch';
import { TEST_INDEX_PREFIX } from './helpers/config';

// eslint-disable-next-line @typescript-eslint/no-unused-vars
import { MUTATE_META, INDEX_META } from '../asset/src/elasticsearch_index_selector/interfaces';

// TODO: current bug in convict prevents testing connection_map without a *
// TODO: test flush scenarios/retries
describe('elasticsearch_bulk', () => {
    const assetDir = path.join(__dirname, '..');
    let harness: WorkerTestHarness;
    let clients: any;
    let clientCalls: any = {};
    const esClient = makeClient();
    const bulkIndex = `${TEST_INDEX_PREFIX}_bulk_`;

    beforeAll(async () => {
        await cleanupIndex(esClient, `${bulkIndex}*`);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, `${bulkIndex}*`);
    });

    function proxyClient(endpoint: string) {
        const client = makeClient();
        const bulkFn = esClient.bulk.bind(client);
        client.bulk = (...args: any[]) => {
            clientCalls[endpoint] = args;
            // @ts-ignore
            return bulkFn(...args);
        };

        return client;
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

    async function makeTest({ opConfig = {}, indexConfig = {}, index = '' } = {}) {
        const testIndex = index ? `${bulkIndex}${index}` : bulkIndex;
        const indexSelctorConfig = Object.assign({
            _op: 'elasticsearch_index_selector',
            index: testIndex,
            preserve_id: false,
            type: 'events'
        }, indexConfig);

        const bulkConfig = Object.assign({ _op: 'elasticsearch_bulk' }, opConfig);

        const job = newTestJobConfig({
            max_retries: 0,
            operations: [
                {
                    _op: 'test-reader',
                    passthrough_slice: true,
                },
                indexSelctorConfig,
                bulkConfig,
            ],
        });

        harness = new WorkerTestHarness(job, { assetDir, clients });
        await harness.initialize();
        return harness;
    }

    it('schema has defaults', async () => {
        const opName = 'elasticsearch_bulk';
        const test = await makeTest();
        const { opConfig: { multisend, size } } = test.getOperation(opName);

        expect(size).toEqual(500);
        expect(multisend).toEqual(false);
    });

    it('if no docs, returnsan empty array', async () => {
        const test = await makeTest();
        const results = await test.runSlice([]);

        expect(results).toEqual([]);
    });

    it('if no docs, returns an empty array', async () => {
        const test = await makeTest();
        const results = await test.runSlice([]);

        expect(results).toEqual([]);
    });

    it('returns the data passed in, with metadata attached', async () => {
        const data = [{ some: 'data' }, { other: 4 }];
        const test = await makeTest({ index: 'docs_returned' });
        const results = await test.runSlice(data);

        expect(results).toEqual(data);

        const indexMetaData = results[0].getMetadata(INDEX_META);
        expect(indexMetaData).toBeDefined();
        expect(indexMetaData).toMatchObject({ index: { _index: 'es_assets__bulk_docs_returned', _type: 'events' } });
    });

    it('does not split if the size is <= than 2 * size in opConfig', async () => {
        // usually each doc is paired with metadata, thus doubling the size of incoming array,
        // hence we double size
        const opConfig = { _op: 'elasticsearch_bulk', size: 50, multisend: false };
        const index = 'less_than_size';
        const query: SearchParams = {
            index: `${bulkIndex}${index}`,
            size: 200
        };
        const data = [];

        for (let i = 0; i < 50; i += 1) {
            data.push({ some: 'data' });
        }

        const test = await makeTest({ opConfig, index });
        const results = await test.runSlice(data);

        expect(Array.isArray(results)).toEqual(true);
        expect(results).toEqual(data);

        await pDelay(1000);

        const fetchedData = await fetch(esClient, query);

        expect(Array.isArray(fetchedData)).toEqual(true);
        expect(fetchedData).toEqual(data);
    });

    it('does split if the size is greater than 2 * size in opConfig', async () => {
        // usually each doc is paired with metadata, thus doubling the size of incoming array,
        // hence we double size
        const opConfig = { _op: 'elasticsearch_bulk', size: 50, multisend: false };
        const index = 'greater_than_size';
        const query: SearchParams = {
            index: `${bulkIndex}${index}`,
            size: 200
        };
        const data = [];

        for (let i = 0; i < 100; i += 1) {
            data.push({ some: 'data' });
        }

        const test = await makeTest({ opConfig, index });
        const results = await test.runSlice(data);

        expect(Array.isArray(results)).toEqual(true);
        expect(results).toEqual(data);

        await pDelay(1000);

        const fetchedData = await fetch(esClient, query);

        expect(Array.isArray(fetchedData)).toEqual(true);
        expect(fetchedData).toEqual(data);
    });

    it('can send delete bulk calls that are over size', async () => {
        const opConfig = { _op: 'elasticsearch_bulk', size: 2, multisend: false };
        const data = [{ some: 'data' }, { other: 'data' }, { final: 'data' }]
            .map((record, index) => {
                const newData = DataEntity.make(record);
                newData.setKey(index + 1);
                return newData;
            });

        const index = `${bulkIndex}deleted_records`;

        const indexConfig = {
            delete: true,
            preserve_id: true,
            index
        };

        const query: SearchParams = {
            index,
            size: 200
        };

        await upload(esClient, { index, type: 'events' }, data);

        const test = await makeTest({ opConfig, indexConfig, index });

        const reader = harness.getOperation('test-reader');
        // @ts-ignore
        const fn = reader.fetch.bind(reader);
        // NOTE: we do not have a good story around added meta data to testing data
        // @ts-ignore
        reader.fetch = async (_incDocs: DataEntity[]) => fn(data);
        const results = await test.runSlice(data);

        expect(Array.isArray(results)).toEqual(true);
        expect(results).toEqual(data);

        await pDelay(1000);

        const fetchedData = await fetch(esClient, query);

        expect(Array.isArray(fetchedData)).toEqual(true);
        expect(fetchedData).toEqual([]);
    });

    it('will throw if connection_map values do not exists in connector config', async () => {
        const opConfig = {
            _op: 'elasticsearch_bulk',
            size: 5,
            multisend: true,
            connection_map: {
                a: 'NotInConnector'
            }
        };
        const errMsg = 'A connection for [NotInConnector] was set on the elasticsearch_bulk connection_map but is not found in the system configuration [terafoundation.connectors.elasticsearch]';

        await expect(makeTest({ opConfig })).rejects.toThrowError(errMsg);
    });

    it('can multisend to several places', async () => {
        const opConfig = {
            _op: 'elasticsearch_bulk',
            size: 5,
            multisend: true,
            connection_map: {
                a: 'default',
                b: 'otherConnection'
            }
        };
        const ids = ['abc', 'aef', 'bde'];

        const data = [{ some: 'data' }, { other: 'data' }, { final: 'data' }]
            .map((record, index) => {
                const newData = DataEntity.make(record);
                const key = ids[index];
                newData.setKey(key);
                return newData;
            });

        const index = `${bulkIndex}multisend`;

        const indexConfig = {
            preserve_id: true,
            index
        };

        const query: SearchParams = {
            index,
            size: 200
        };

        const test = await makeTest({ opConfig, indexConfig, index });

        const reader = harness.getOperation('test-reader');
        // @ts-ignore
        const fn = reader.fetch.bind(reader);
        // NOTE: we do not have a good story around added meta data to testing data
        // @ts-ignore
        reader.fetch = async (_incDocs: DataEntity[]) => fn(data);

        await test.runSlice(data);

        expect(clientCalls.default).toBeDefined();
        expect(clientCalls.otherConnection).toBeDefined();
        const {
            default: [
                { body: [_meta1, doc1, _meta2, doc2] }
            ],
            otherConnection: [
                { body: [_meta3, doc3] }
            ]
        } = clientCalls;

        expect([doc1, doc2, doc3]).toEqual(data);

        await pDelay(1000);

        const fetchedData = await fetch(esClient, query) as any[];

        expect(Array.isArray(fetchedData)).toEqual(true);
        expect(fetchedData.length).toEqual(3);
    });

    it('multisend_index_append will change outgoing _id', async () => {
        const opConfig = {
            _op: 'elasticsearch_bulk',
            size: 5,
            multisend: true,
            multisend_index_append: true,
            connection_map: {
                a: 'default'
            }
        };

        const ids = ['abc', 'aef', 'ade'];

        const data = [{ some: 'data' }, { other: 'data' }, { final: 'data' }]
            .map((record, index) => {
                const newData = DataEntity.make(record);
                const key = ids[index];
                newData.setKey(key);
                return newData;
            });

        const index = `${bulkIndex}multisend_append`;
        const expectedIndex = `${index}-a`;
        const indexConfig = {
            preserve_id: true,
            index
        };

        const query: SearchParams = {
            index: expectedIndex,
            size: 200
        };

        const test = await makeTest({ opConfig, indexConfig, index });

        const reader = harness.getOperation('test-reader');
        // @ts-ignore
        const fn = reader.fetch.bind(reader);
        // NOTE: we do not have a good story around added meta data to testing data
        // @ts-ignore
        reader.fetch = async (_incDocs: DataEntity[]) => fn(data);

        await test.runSlice(data);
        // const results = await test.runSlice(data);

        const { default: [{ body }] } = clientCalls;

        const isAppended = body
            .filter((obj: any) => obj.index !== undefined)
            .map((meta: any) => meta.index._index)
            .every((indexName: any) => indexName === expectedIndex);

        expect(isAppended).toEqual(true);

        await pDelay(1000);

        const fetchedData = await fetch(esClient, query) as any[];
        expect(fetchedData.length).toEqual(3);
    });
});
