import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { AnyObject } from '@terascope/job-components';
import { ESIDReaderConfig } from '../../asset/src/id_reader/interfaces';
import { DEFAULT_API_NAME } from '../../asset/src/elasticsearch_reader_api/interfaces';

describe('Id Reader Schema', () => {
    const index = 'some_index';
    const name = 'id_reader';
    const field = 'someField';

    const clients = [
        {
            type: 'elasticsearch',
            endpoint: 'default',
            create: () => ({
                client: {}
            }),
        },
    ];

    let harness: WorkerTestHarness;

    async function makeSchema(config: AnyObject = {}): Promise<ESIDReaderConfig> {
        const opConfig = Object.assign({}, { _op: name, index, field }, config);
        harness = WorkerTestHarness.testFetcher(opConfig, { clients });

        await harness.initialize();

        const validConfig = harness.executionContext.config.operations.find(
            (testConfig) => testConfig._op === name
        );

        return validConfig as ESIDReaderConfig;
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
                    { _name: DEFAULT_API_NAME, index }
                ],
                operations: [
                    { _op: name, index },
                    { _op: 'noop' }
                ]
            });

            harness = new WorkerTestHarness(job, { clients });

            await expect(harness.initialize()).toResolve();
        });
    });
});
