import 'jest-extended';
import { newTestJobConfig, APIConfig, TestClientConfig } from '@terascope/job-components';
import { debugLogger } from '@terascope/core-utils';
import { WorkerTestHarness } from 'teraslice-test-harness';
import { makeClient } from '../helpers/index.js';
import { DEFAULT_API_NAME } from '../../asset/src/spaces_reader_api/interfaces.js';

describe('spaces-reader schema', () => {
    let harness: WorkerTestHarness;
    const index = 'some_index';
    const name = 'spaces_reader';
    const logger = debugLogger('test-logger');

    let esClient: any;
    let clients: TestClientConfig[];

    beforeAll(async () => {
        esClient = await makeClient();

        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: esClient,
                    logger
                }),
            }
        ];
    });

    async function makeTest(config: Record<string, any>) {
        const apiConfig = Object.assign({
            _name: 'spaces_reader_api',
        }, config);

        const partialJob: Record<string, any> = {
            name: 'simple-api-reader-job',
            apis: [apiConfig],
            operations: [
                { _op: 'spaces_reader', _api_name: 'spaces_reader_api' },
                { _op: 'noop' }
            ]
        };

        harness = new WorkerTestHarness(newTestJobConfig(partialJob), {});

        await harness.initialize();

        return harness;
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    it('can properly instantiate', async () => {
        const apiName = 'spaces_reader_api';
        const config = {
            index: 'test_index',
            date_field_name: 'created',
            endpoint: '127.0.0.1',
            token: 'someToken'
        };
        const test = await makeTest(config);

        const op = test.getOperation('spaces_reader');
        const apiManager = test.getAPI(apiName);

        if (op == null) throw new Error('Could not find spaces_reader');
        if (apiManager == null) throw new Error('Could not find spaces_reader_api');

        const apiConfig = apiManager.getConfig(apiName);
        const { _api_name } = op.opConfig;

        expect(_api_name).toEqual(apiName);
        expect(apiConfig).toMatchObject(config);
    });

    it('should not throw if all connection config is on api', async () => {
        const testAPIConfig = {
            _name: DEFAULT_API_NAME,
            index,
            endpoint: '127.0.0.1',
            token: 'someToken',
            date_field_name: 'created',
        };

        const job = newTestJobConfig({
            apis: [testAPIConfig],
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

        expect(validatedApiConfig).toMatchObject(testAPIConfig);
    });
});
