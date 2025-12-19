import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { TestClientConfig, APIConfig } from '@terascope/job-components';
import { debugLogger } from '@terascope/core-utils';
import { TEST_INDEX_PREFIX, makeClient } from '../helpers/index.js';
import { ElasticsearchReaderAPIConfig, DEFAULT_API_NAME } from '../../asset/src/elasticsearch_reader_api/interfaces.js';

describe('elasticsearch reader api schema', () => {
    const apiSenderIndex = `${TEST_INDEX_PREFIX}_elasticsearch_reader_api_schema_`;
    const logger = debugLogger('test-logger');

    const apiName = DEFAULT_API_NAME;
    const harnesses: WorkerTestHarness[] = [];

    let harness: WorkerTestHarness;
    let clients: TestClientConfig[];
    let esClient: any;

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
            }
        ];
    });

    async function makeSchema(
        config: Record<string, any> = {}
    ): Promise<ElasticsearchReaderAPIConfig> {
        const defaults = {
            _name: apiName,
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
                    apiName: apiName
                }
            ],
        });

        harness = new WorkerTestHarness(job, { clients });

        await harness.initialize();
        harnesses.push(harness);

        const { apis } = harness.executionContext.config;

        return apis.find(
            (settings: APIConfig) => settings._name === apiName
        ) as ElasticsearchReaderAPIConfig;
    }

    afterEach(async () => {
        await Promise.all(harnesses.map((testHarness) => testHarness.shutdown()));
    });

    it('should have defaults', async () => {
        const { _connection } = await makeSchema({ index: apiSenderIndex });

        expect(_connection).toEqual('default');
    });

    it('should values are incorrect', async () => {
        await expect(makeSchema({ _connection: -4 })).toReject();
        await expect(makeSchema({ index: -3 })).toReject();
        await expect(makeSchema({ index: undefined })).toReject();
    });

    it('subslice_by_key configuration validation', async () => {
        const badOP = { subslice_by_key: true };
        const goodOP = { subslice_by_key: true, id_field_name: 'events-' };
        const otherGoodOP = { subslice_by_key: false, id_field_name: 'events-' };
        // NOTE: geo self validations are tested in elasticsearch_api module

        const testOpConfig = {
            _op: 'elasticsearch_reader',
            index: 'some-index',
            date_field_name: 'created'
        };

        await expect(makeSchema(Object.assign({}, testOpConfig, badOP))).toReject();

        const goodOp = await makeSchema(Object.assign({}, testOpConfig, goodOP));
        expect(goodOp).toBeObject();

        const goodOp2 = await makeSchema(Object.assign({}, testOpConfig, otherGoodOP));
        expect(goodOp2).toBeObject();
    });

    it('should throw if in subslice_by_key is set but type is not in elasticsearch <= v5', async () => {
        await expect(makeSchema({ subslice_by_key: true })).toReject();
        await expect(makeSchema({ subslice_by_key: true, id_field_name: 'hello' })).toResolve();
    });
});
