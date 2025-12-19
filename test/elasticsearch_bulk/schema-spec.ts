import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { OpConfig, TestClientConfig, APIConfig } from '@terascope/job-components';
import { debugLogger } from '@terascope/core-utils';
import { ElasticsearchBulkConfig } from '../../asset/src/elasticsearch_bulk/interfaces.js';
import { DEFAULT_API_NAME } from '../../asset/src/elasticsearch_sender_api/interfaces.js';

describe('Elasticsearch Bulk Schema', () => {
    const index = 'some_index';
    const name = 'elasticsearch_bulk';
    const logger = debugLogger('test-logger');

    const clients: TestClientConfig[] = [
        {
            type: 'elasticsearch-next',
            endpoint: 'default',
            createClient: async () => ({
                client: {},
                logger
            }),
        },
    ];

    let harness: WorkerTestHarness;

    async function makeSchema(config: Record<string, any> = {}): Promise<ElasticsearchBulkConfig> {
        const opConfig = Object.assign({}, { _op: name, _api_name: 'elasticsearch_sender_api' }, config);
        harness = WorkerTestHarness.testSender(opConfig, { _name: 'elasticsearch_sender_api', index, _connection: 'default' }, { clients });

        await harness.initialize();

        const validConfig = harness.executionContext.config.operations.find(
            (testConfig: OpConfig) => testConfig._op === name
        );

        return validConfig as unknown as ElasticsearchBulkConfig;
    }

    afterEach(async () => {
        if (harness) {
            await harness.shutdown();
        }
    });

    describe('when validating the schema', () => {
        it('should have defaults', async () => {
            const { _api_name } = await makeSchema({});

            expect(_api_name).toEqual(DEFAULT_API_NAME);
        });

        it('should not throw if api is created but opConfig has index set', async () => {
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

        it('should not throw if all connection config is on api', async () => {
            const job = newTestJobConfig({
                apis: [
                    { _name: DEFAULT_API_NAME, index }
                ],
                operations: [
                    { _op: 'test-reader' },
                    { _op: name, _api_name: DEFAULT_API_NAME }
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
                size: 500
            });
        });
    });
});
