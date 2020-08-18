import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { AnyObject, ValidatedJobConfig } from '@terascope/job-components';
import { getESVersion } from 'elasticsearch-store';
import { ESIDReaderConfig } from '../../asset/src/id_reader/interfaces';
import { DEFAULT_API_NAME } from '../../asset/src/elasticsearch_reader_api/interfaces';
import { makeClient, ELASTICSEARCH_VERSION } from '../helpers';

describe('id_reader Schema', () => {
    const index = 'some_index';
    const name = 'id_reader';
    const field = 'someField';

    const esClient = makeClient();

    const clients = [
        {
            type: 'elasticsearch',
            endpoint: 'default',
            create: () => ({
                client: esClient
            }),
            config: {
                apiVersion: ELASTICSEARCH_VERSION
            }
        },
    ];

    const version = getESVersion(esClient);

    const docType = version === 5 ? 'events' : '_doc';

    let harness: WorkerTestHarness;

    async function makeSchema(config: AnyObject = {}): Promise<ESIDReaderConfig> {
        const base: AnyObject = {};
        if (version <= 5) base.type = docType;
        const opConfig = Object.assign(base, { _op: name, index, field }, config);
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

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('when validating the schema', () => {
        it('should have defaults', async () => {
            const {
                size,
                key_type,
                connection,
                api_name
            } = await makeSchema({ index });

            expect(size).toEqual(10000);
            expect(key_type).toEqual('base64url');
            expect(connection).toEqual('default');
            expect(api_name).toEqual(DEFAULT_API_NAME);
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

        it('should throw if api is created but opConfig has index set', async () => {
            const job = newTestJobConfig({
                apis: [
                    { _name: DEFAULT_API_NAME, index, type: docType }
                ],
                operations: [
                    { _op: name, index },
                    { _op: 'noop' }
                ]
            });

            harness = new WorkerTestHarness(job, { clients });

            await expect(harness.initialize()).toResolve();
        });

        it('can validateJob to make sure its configured correctly', async () => {
            const job1 = newTestJobConfig({
                slicers: 1,
                operations: [{
                    _op: 'id_reader', type: docType, index: 'some-index', key_range: ['a', 'b']
                }, { _op: 'noop' }]
            });
            const job2 = newTestJobConfig({
                slicers: 2,
                operations: [{
                    _op: 'id_reader', type: docType, index: 'some-index', key_range: ['a']
                }, { _op: 'noop' }]
            });
            const job3 = newTestJobConfig({
                slicers: 4,
                operations: [{
                    _op: 'id_reader', type: docType, index: 'some-index', key_type: 'hexadecimal'
                }, { _op: 'noop' }]
            });
            const job4 = newTestJobConfig({
                slicers: 20,
                operations: [{
                    _op: 'id_reader', type: docType, index: 'some-index', key_type: 'hexadecimal'
                }, { _op: 'noop' }]
            });
            const job5 = newTestJobConfig({
                slicers: 20,
                operations: [{
                    _op: 'id_reader', type: docType, index: 'some-index', key_type: 'base64url'
                }, { _op: 'noop' }]
            });
            const job6 = newTestJobConfig({
                slicers: 70,
                operations: [{
                    _op: 'id_reader', type: docType, index: 'some-index', key_type: 'base64url'
                }, { _op: 'noop' }]
            });

            await expect(testValidation(job1)).toResolve();
            await expect(testValidation(job3)).toResolve();
            await expect(testValidation(job5)).toResolve();

            await expect(testValidation(job2)).toReject();
            await expect(testValidation(job4)).toReject();
            await expect(testValidation(job6)).toReject();
        });

        it('will throw if version is <=5 and no type is specified', async () => {
            if (version <= 5) {
                await expect(makeSchema({ type: '' })).toReject();
                await expect(makeSchema({ type: null })).toReject();
                await expect(makeSchema({ type: ['hello', 'world'] })).toReject();
                await expect(makeSchema({ type: docType })).toResolve();
            } else {
                await expect(makeSchema({})).toResolve();
            }
        });
    });
});
