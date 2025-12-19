import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { ElasticsearchTestHelpers } from '@terascope/opensearch-client';
import {
    ValidatedJobConfig, OpConfig, APIConfig, TestClientConfig
} from '@terascope/job-components';
import { debugLogger } from '@terascope/core-utils';
import { ESIDReaderConfig } from '../../asset/src/id_reader/interfaces.js';
import { DEFAULT_API_NAME } from '../../asset/src/elasticsearch_reader_api/interfaces.js';
import {
    TEST_INDEX_PREFIX, makeClient, cleanupIndex,
    populateIndex
} from '../helpers/index.js';

describe('id_reader Schema', () => {
    const name = 'id_reader';
    const id_field_name = 'someField';
    const logger = debugLogger('test-logger');

    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_id_reader_schema`;

    function makeIndex(str: string) {
        return `${readerIndex}_${str}`;
    }

    const evenSpread = ElasticsearchTestHelpers.EvenDateData;

    const index = makeIndex('even_data');

    const evenBulkData = evenSpread.data;

    let harness: WorkerTestHarness;
    let esClient: any;
    let clients: TestClientConfig[];

    async function makeSchema(config: Record<string, any> = {}): Promise<ESIDReaderConfig> {
        const apiConfig = Object.assign({}, { _name: 'elasticsearch_reader_api', id_field_name }, config) as any;
        harness = WorkerTestHarness.testFetcher(
            { _op: name, _api_name: 'elasticsearch_reader_api' },
            apiConfig,
            { clients }
        );

        await harness.initialize();

        const validConfig = harness.executionContext.config.operations.find(
            (testConfig: OpConfig) => testConfig._op === name
        );

        return validConfig as ESIDReaderConfig;
    }

    async function testValidation(job: ValidatedJobConfig) {
        harness = new WorkerTestHarness(job, { clients });
        await harness.initialize();
    }

    beforeAll(async () => {
        esClient = await makeClient();

        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: esClient,
                    logger
                })
            },
        ];
        await cleanupIndex(esClient, makeIndex('*'));
        await populateIndex(esClient, index, evenSpread.EvenDataType, evenBulkData);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, makeIndex('*'));
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('when validating the schema', () => {
        it('should have defaults', async () => {
            const { _api_name } = await makeSchema({ index });

            expect(_api_name).toEqual('elasticsearch_reader_api');
        });

        it('should throw if index is incorrect', async () => {
            await expect(makeSchema({ index: 4 })).toReject();
            await expect(makeSchema({ index: '' })).toReject();
            await expect(makeSchema({ index: 'Hello' })).toReject();
            await expect(makeSchema({ index: 'hello' })).toResolve();
        });

        it('should values are incorrect', async () => {
            await expect(makeSchema({ size: -4 })).toReject();

            await expect(makeSchema({ key_type: 'some key' })).toReject();

            await expect(makeSchema({ key_range: { some: 'stuff' } })).toReject();
            await expect(makeSchema({ key_range: [] })).toReject();
            await expect(makeSchema({ key_range: ['hello', 4] })).toReject();

            await expect(makeSchema({ fields: 'ehh' })).toReject();
            await expect(makeSchema({ fields: ['hello', 2] })).toReject();

            await expect(makeSchema({ _api_name: [1, 2, 3] })).toReject();
            await expect(makeSchema({ _api_name: 'hello' })).toReject();
        });

        it('should throw if api is created but opConfig has index set to another value', async () => {
            const job = newTestJobConfig({
                apis: [
                    { _name: DEFAULT_API_NAME, index }
                ],
                operations: [
                    { _op: name, index: 'something_else', _api_name: DEFAULT_API_NAME },
                    { _op: 'noop' }
                ]
            });

            await expect(async () => {
                const test = new WorkerTestHarness(job, { clients });
                await test.initialize();
            }).rejects.toThrow();
        });

        it('should throw if number of slicers are greater than key_range length', async () => {
            const apiConfig = Object.assign({}, { _name: 'elasticsearch_reader_api', id_field_name }, { index: 'something_else', key_range: ['a', 'b'] });
            const job = newTestJobConfig({
                slicers: 72,
                apis: [apiConfig],
                operations: [
                    { _op: name, _api_name: 'elasticsearch_reader_api' },
                    { _op: 'noop' }
                ]
            });

            await expect(async () => {
                const test = new WorkerTestHarness(job, { clients });
                await test.initialize();
            }).rejects.toThrow();
        });

        it('should throw if number of slicers are greater than key_type length', async () => {
            const apiConfig1 = Object.assign({}, { _name: 'elasticsearch_reader_api', id_field_name }, { index });
            const apiConfig2 = Object.assign({}, { _name: 'elasticsearch_reader_api', id_field_name }, { index, key_type: 'hexadecimal' });

            const job = newTestJobConfig({
                slicers: 72,
                apis: [apiConfig1],
                operations: [
                    { _op: name, _api_name: 'elasticsearch_reader_api' },
                    { _op: 'noop' }
                ]
            });

            await expect(async () => {
                const test = new WorkerTestHarness(job, { clients });
                await test.initialize();
            }).rejects.toThrow();

            const job2 = newTestJobConfig({
                slicers: 17,
                apis: [apiConfig2],
                operations: [
                    { _op: name, _api_name: 'elasticsearch_reader_api' },
                    { _op: 'noop' }
                ]
            });

            await expect(async () => {
                const test = new WorkerTestHarness(job2, { clients });
                await test.initialize();
            }).rejects.toThrow();
        });

        it('can validateJob to make sure its configured correctly', async () => {
            const base = { _name: 'elasticsearch_reader_api', id_field_name, index };
            const apiConfig1 = Object.assign({}, base, { key_range: ['a', 'b'] });
            const apiConfig2 = Object.assign({}, base, { key_range: ['a'] });
            const apiConfig3 = Object.assign({}, base, { key_type: 'hexadecimal' });
            const apiConfig4 = Object.assign({}, base, { key_type: 'hexadecimal' });
            const apiConfig5 = Object.assign({}, base, { key_type: 'base64url' });
            const apiConfig6 = Object.assign({}, base, { key_type: 'base64url' });

            const job1 = newTestJobConfig({
                slicers: 1,
                apis: [apiConfig1],
                operations: [
                    { _op: name, _api_name: 'elasticsearch_reader_api' },
                    { _op: 'noop' }
                ]
            });
            const job2 = newTestJobConfig({
                slicers: 2,
                apis: [apiConfig2],
                operations: [
                    { _op: name, _api_name: 'elasticsearch_reader_api' },
                    { _op: 'noop' }
                ]
            });
            const job3 = newTestJobConfig({
                slicers: 4,
                apis: [apiConfig3],
                operations: [
                    { _op: name, _api_name: 'elasticsearch_reader_api' },
                    { _op: 'noop' }
                ]
            });
            const job4 = newTestJobConfig({
                slicers: 20,
                apis: [apiConfig4],
                operations: [
                    { _op: name, _api_name: 'elasticsearch_reader_api' },
                    { _op: 'noop' }
                ]
            });
            const job5 = newTestJobConfig({
                slicers: 20,
                apis: [apiConfig5],
                operations: [
                    { _op: name, _api_name: 'elasticsearch_reader_api' },
                    { _op: 'noop' }
                ]
            });
            const job6 = newTestJobConfig({
                slicers: 70,
                apis: [apiConfig6],
                operations: [
                    { _op: name, _api_name: 'elasticsearch_reader_api' },
                    { _op: 'noop' }
                ]
            });

            await expect(testValidation(job1)).toResolve();
            await expect(testValidation(job3)).toResolve();
            await expect(testValidation(job5)).toResolve();

            await expect(testValidation(job2)).toReject();
            await expect(testValidation(job4)).toReject();
            await expect(testValidation(job6)).toReject();
        });
    });

    it('should not throw if all connection config is on api', async () => {
        const job = newTestJobConfig({
            apis: [
                { _name: DEFAULT_API_NAME, index, id_field_name }
            ],
            operations: [
                {
                    _op: name,
                    _api_name: DEFAULT_API_NAME
                },
                { _op: 'noop' }
            ]
        });

        harness = new WorkerTestHarness(job, { clients });

        await harness.initialize();

        const validatedApiConfig = harness.executionContext.config.apis.find(
            (api: APIConfig) => api._name === DEFAULT_API_NAME
        );

        expect(validatedApiConfig).toMatchObject({
            _name: DEFAULT_API_NAME,
            index,
            size: 5000
        });
    });
});
