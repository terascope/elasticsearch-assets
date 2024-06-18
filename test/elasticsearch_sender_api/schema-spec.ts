import 'jest-extended';
import path from 'path';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { TestContext, APIConfig, Context } from '@terascope/job-components';
import { TEST_INDEX_PREFIX, makeClient } from '../../dist/test/helpers/index.js';
import { ElasticsearchSenderAPI, DEFAULT_API_NAME } from '../../asset/src/elasticsearch_sender_api/interfaces.js';
import SenderSchema from '../../asset/src/elasticsearch_sender_api/schema.js';

describe('elasticsearch sender api schema', () => {
    const apiSenderIndex = `${TEST_INDEX_PREFIX}_elasticsearch_sender_api_schema_`;
    const docType = '_doc';

    let harness: WorkerTestHarness;
    let esClient: any;
    let clients: any;

    beforeAll(async () => {
        esClient = await makeClient();

        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: esClient
                }),
            }
        ];
    });

    const apiName = DEFAULT_API_NAME;

    async function makeSchema(config: Record<string, any> = {}): Promise<ElasticsearchSenderAPI> {
        const defaults = {
            _name: apiName,
            type: docType,
            index: apiSenderIndex,
        };

        const apiConfig = Object.assign({}, defaults, config);

        const job = newTestJobConfig({
            max_retries: 3,
            apis: [apiConfig],
            operations: [
                {
                    _op: 'test-reader',
                    passthrough_slice: true
                },
                {
                    _op: 'noop',
                    apiName: 'elasticsearch_sender_api'
                }
            ],
        });

        harness = new WorkerTestHarness(job, { clients });
        await harness.initialize();

        const { apis } = harness.executionContext.config;
        return apis.find(
            (settings: APIConfig) => settings._name === apiName
        ) as ElasticsearchSenderAPI;
    }

    afterEach(async () => {
        await harness.shutdown();
    });

    it('should have defaults', async () => {
        const {
            size,
            type,
            create,
            upsert,
            update_fields
        } = await makeSchema({ index: apiSenderIndex });

        expect(size).toEqual(500);
        expect(type).toEqual(docType);
        expect(create).toEqual(false);
        expect(upsert).toEqual(false);
        expect(update_fields).toBeArrayOfSize(0);
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
        await expect(makeSchema({ size: { some: 'stuff' } })).toReject();
    });
});

describe('elasticsearch sender api schema for routed sender jobs', () => {
    let harness: WorkerTestHarness;
    let context: TestContext;

    beforeAll(async () => {
        // need this to create valid context to remove the default es connection
        const esClient = await makeClient();

        const clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'test-es',
                createClient: async () => ({
                    client: esClient
                }),
            },
            {
                type: 'elasticsearch-next',
                endpoint: 'test-es1',
                createClient: async () => ({
                    client: esClient
                }),
            }
        ];

        const testAsset = path.join(__dirname, '..', 'fixtures');

        const initialJob = newTestJobConfig({
            max_retries: 3,
            apis: [
                {
                    _name: 'elasticsearch_sender_api',
                    index: 'test-index',
                }
            ],
            operations: [
                {
                    _op: 'test-reader',
                    passthrough_slice: true
                },
                {
                    _op: 'routed_sender',
                    api_name: 'elasticsearch_sender_api',
                    routing: {
                        '**': 'test-es'
                    }
                }
            ],
        });

        harness = new WorkerTestHarness(initialJob, {
            clients,
            assetDir: [
                testAsset,
                process.cwd()
            ]
        });

        context = harness.context;
    });

    afterEach(async () => {
        await harness.shutdown();
    });

    it('should not throw if default is not an es endpoint and routed sender is an operation', async () => {
        const schema = new SenderSchema(context as unknown as Context);

        const job = newTestJobConfig({
            max_retries: 3,
            apis: [
                {
                    _name: 'elasticsearch_sender_api',
                    connection: 'default',
                    index: 'test-index',
                }
            ],
            operations: [
                {
                    _op: 'test-reader',
                    passthrough_slice: true
                },
                {
                    _op: 'routed_sender',
                    api_name: 'elasticsearch_sender_api',
                    routing: {
                        '**': 'test-es'
                    }
                }
            ],
        });

        expect(() => schema.validateJob(job)).not.toThrow();
    });

    it('should not throw if default is not an es endpoint and multiple routed sender operations', async () => {
        const schema = new SenderSchema(context as unknown as Context);

        const job = newTestJobConfig({
            max_retries: 3,
            apis: [
                {
                    _name: 'elasticsearch_sender_api',
                    connection: 'test-es1',
                    index: 'test-index',
                },
                {
                    _name: 'elasticsearch_sender_api:routed1',
                    connection: 'default',
                    index: 'test-index',
                },
                {
                    _name: 'elasticsearch_sender_api:routed2',
                    connection: 'default',
                    index: 'test-index',
                }
            ],
            operations: [
                {
                    _op: 'test-reader',
                    passthrough_slice: true
                },
                {
                    _op: 'routed_sender',
                    api_name: 'elasticsearch_sender_api:routed1',
                    routing: {
                        a: 'test-es',
                        b: 'test-es2',
                        '*': 'test-es3',
                    }
                },
                {
                    _op: 'routed_sender',
                    api_name: 'elasticsearch_sender_api:routed2',
                    routing: {
                        '**': 'test-es'
                    }
                },
                {
                    _op: 'noop',
                    api_name: 'elasticsearch_sender_api'
                }
            ],
        });

        expect(() => schema.validateJob(job)).not.toThrow();
    });

    it('should throw if no valid connection is defined', async () => {
        const job2 = newTestJobConfig({
            max_retries: 3,
            apis: [
                {
                    _name: 'elasticsearch_sender_api',
                    connection: 'test-whatever',
                    index: 'test-index',
                }
            ],
            operations: [
                {
                    _op: 'test-reader',
                    passthrough_slice: true
                },
                {
                    _op: 'noop',
                    api_name: 'elasticsearch_sender_api'
                }
            ],
        });

        const schema = new SenderSchema(context as unknown as Context);

        expect(() => schema.validateJob(job2)).toThrow();
    });
});
