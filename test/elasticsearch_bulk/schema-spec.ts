import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { AnyObject } from '@terascope/job-components';
import { ElasticsearchBulkConfig } from '../../asset/src/elasticsearch_bulk/interfaces';
import { DEFAULT_API_NAME } from '../../asset/src/elasticsearch_sender_api/interfaces';

describe('Elasticsearch Bulk Schema', () => {
    const index = 'some_index';
    const name = 'elasticsearch_bulk';

    const clients = [
        {
            type: 'elasticsearch-next',
            endpoint: 'default',
            createClient: async () => ({
                client: {}
            }),
        },
    ];

    let harness: WorkerTestHarness;

    async function makeSchema(config: AnyObject = {}): Promise<ElasticsearchBulkConfig> {
        const opConfig = Object.assign({}, { _op: name, index }, config);
        harness = WorkerTestHarness.testProcessor(opConfig, { clients });

        await harness.initialize();

        const validConfig = harness.executionContext.config.operations.find(
            (testConfig) => testConfig._op === name
        );

        return validConfig as ElasticsearchBulkConfig;
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('when validating the schema', () => {
        it('should have defaults', async () => {
            const {
                size,
                type,
                create,
                upsert,
                update_fields,
                api_name
            } = await makeSchema({ index });

            expect(size).toEqual(500);
            expect(type).toEqual('_doc');
            expect(create).toEqual(false);
            expect(upsert).toEqual(false);
            expect(update_fields).toBeArrayOfSize(0);
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
            await expect(makeSchema({ update_retry_on_conflict: -3 })).toReject();
            await expect(makeSchema({ delete: { some: 'stuff' } })).toReject();
            await expect(makeSchema({ api_name: [1, 2, 3] })).toReject();
        });

        it('should throw if api is created but opConfig has index set', async () => {
            const job = newTestJobConfig({
                apis: [
                    { _name: DEFAULT_API_NAME, index }
                ],
                operations: [
                    { _op: 'test-reader' },
                    { _op: name, index }
                ]
            });

            harness = new WorkerTestHarness(job, { clients });

            await expect(harness.initialize()).toResolve();
        });
    });
});
