import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { AnyObject, ValidatedJobConfig, DataEntity } from '@terascope/job-components';
import { ESIDReaderConfig } from '../../asset/src/id_reader/interfaces';
import { DEFAULT_API_NAME } from '../../asset/src/elasticsearch_reader_api/interfaces';
import {
    TEST_INDEX_PREFIX,
    makeClient,
    cleanupIndex,
    populateIndex
} from '../helpers';
import evenSpread from '../fixtures/data/even-spread';

describe('id_reader Schema', () => {
    const name = 'id_reader';
    const id_field_name = 'someField';

    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_id_reader_schema`;

    function makeIndex(str: string) {
        return `${readerIndex}_${str}`;
    }

    const docType = '_doc';

    const index = makeIndex(evenSpread.index);

    const evenBulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    let harness: WorkerTestHarness;
    let esClient: any;
    let clients: any;

    async function makeSchema(config: AnyObject = {}): Promise<ESIDReaderConfig> {
        const base: AnyObject = {};
        const opConfig = Object.assign(base, { _op: name, index, id_field_name }, config);
        harness = WorkerTestHarness.testFetcher(opConfig, { clients });

        await harness.initialize();

        const validConfig = harness.executionContext.config.operations.find(
            (testConfig) => testConfig._op === name
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
                    client: esClient
                })
            },
        ];
        await cleanupIndex(esClient, makeIndex('*'));
        await populateIndex(esClient, index, evenSpread.types, evenBulkData, docType);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, makeIndex('*'));
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('when validating the schema', () => {
        it('should have defaults', async () => {
            const { api_name } = await makeSchema({ index });

            expect(api_name).toEqual('elasticsearch_reader_api:id_reader-0');
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

            await expect(makeSchema({ api_name: [1, 2, 3] })).toReject();
            await expect(makeSchema({ api_name: 'hello' })).toReject();
        });

        it('should throw if api is created but opConfig has index set to another value', async () => {
            const job = newTestJobConfig({
                apis: [
                    { _name: DEFAULT_API_NAME, index, type: docType }
                ],
                operations: [
                    { _op: name, index: 'something_else', api_name: DEFAULT_API_NAME },
                    { _op: 'noop' }
                ]
            });

            expect(() => new WorkerTestHarness(job, { clients })).toThrow();
        });

        it('should not throw if base api is created but opConfig has index set to another value', async () => {
            const job = newTestJobConfig({
                apis: [
                    { _name: DEFAULT_API_NAME, index, type: docType }
                ],
                operations: [
                    { _op: name, index, type: docType },
                    { _op: 'noop' }
                ]
            });

            harness = new WorkerTestHarness(job, { clients });

            await harness.initialize();

            const apiConfig = harness.executionContext.config.apis.find(
                (api) => api._name === 'elasticsearch_reader_api:id_reader-0'
            );

            expect(apiConfig).toMatchObject({ index });
        });

        it('should throw if number of slicers are greater than key_range length', async () => {
            const job = newTestJobConfig({
                slicers: 72,
                operations: [
                    {
                        _op: name,
                        index: 'something_else',
                        api_name: DEFAULT_API_NAME,
                        key_range: ['a', 'b']
                    },
                    { _op: 'noop' }
                ]
            });

            expect(() => new WorkerTestHarness(job, { clients })).toThrow();
        });

        it('should throw if number of slicers are greater than key_type length', async () => {
            const job = newTestJobConfig({
                slicers: 72,
                operations: [
                    { _op: name, index },
                    { _op: 'noop' }
                ]
            });

            expect(() => new WorkerTestHarness(job, { clients })).toThrow();

            const job2 = newTestJobConfig({
                slicers: 17,
                operations: [
                    { _op: name, index, key_type: 'hexadecimal' },
                    { _op: 'noop' }
                ]
            });

            expect(() => new WorkerTestHarness(job2, { clients })).toThrow();
        });

        it('can validateJob to make sure its configured correctly', async () => {
            const job1 = newTestJobConfig({
                slicers: 1,
                operations: [{
                    _op: 'id_reader', type: docType, index, key_range: ['a', 'b']
                }, { _op: 'noop' }]
            });
            const job2 = newTestJobConfig({
                slicers: 2,
                operations: [{
                    _op: 'id_reader', type: docType, index, key_range: ['a']
                }, { _op: 'noop' }]
            });
            const job3 = newTestJobConfig({
                slicers: 4,
                operations: [{
                    _op: 'id_reader', type: docType, index, key_type: 'hexadecimal'
                }, { _op: 'noop' }]
            });
            const job4 = newTestJobConfig({
                slicers: 20,
                operations: [{
                    _op: 'id_reader', type: docType, index, key_type: 'hexadecimal'
                }, { _op: 'noop' }]
            });
            const job5 = newTestJobConfig({
                slicers: 20,
                operations: [{
                    _op: 'id_reader', type: docType, index, key_type: 'base64url'
                }, { _op: 'noop' }]
            });
            const job6 = newTestJobConfig({
                slicers: 70,
                operations: [{
                    _op: 'id_reader', type: docType, index, key_type: 'base64url'
                }, { _op: 'noop' }]
            });

            await expect(testValidation(job1)).toResolve();
            await expect(testValidation(job3)).toResolve();
            await expect(testValidation(job5)).toResolve();

            await expect(testValidation(job2)).toReject();
            await expect(testValidation(job4)).toReject();
            await expect(testValidation(job6)).toReject();
        });
    });
});
