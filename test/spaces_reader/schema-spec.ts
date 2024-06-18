import 'jest-extended';
import {
    newTestJobConfig, AnyObject, APIConfig,
    debugLogger, TestClientConfig
} from '@terascope/job-components';
import { WorkerTestHarness } from 'teraslice-test-harness';
import { makeClient } from '../helpers/index.js';
import { DEFAULT_API_NAME } from '../../asset/src/spaces_reader_api/interfaces.js';

describe('spaces-reader schema', () => {
    let harness: WorkerTestHarness;
    const index = 'some_index';
    const name = 'spaces_reader';
    const logger = debugLogger('test-logger');
    const docType = '_doc';

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

    async function makeTest(opConfig: AnyObject, apiConfig?: AnyObject) {
        const readerConfig = Object.assign({
            _op: 'spaces_reader',
        }, opConfig);

        const partialJob: AnyObject = {
            name: 'simple-api-reader-job',
            apis: [],
            operations: [
                readerConfig,
                {
                    _op: 'noop'
                }
            ]
        };

        if (apiConfig) {
            const newAPIConfig = Object.assign({ _name: 'spaces_reader_api' }, apiConfig);
            partialJob.apis.push(newAPIConfig);
        }

        harness = new WorkerTestHarness(newTestJobConfig(partialJob), {});

        await harness.initialize();

        return harness;
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    it('can properly instantiate', async () => {
        const apiName = 'spaces_reader_api:spaces_reader-0';
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
        if (apiManager == null) throw new Error('Could not find spaces_reader_api:spaces_reader-0');

        const apiConfig = apiManager.getConfig(apiName);
        const { api_name } = op.opConfig;

        expect(api_name).toEqual(apiName);
        expect(apiConfig).toMatchObject(config);
    });

    it('will not throw if parameters are in api', async () => {
        const apiConfig = {
            index: 'test_index',
            date_field_name: 'created',
            endpoint: '127.0.0.1',
            token: 'someToken'
        };

        await expect(makeTest({ api_name: 'spaces_reader_api' }, apiConfig)).toResolve();
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

        await expect(async () => {
            const test = new WorkerTestHarness(job, { clients });
            await test.initialize();
        }).rejects.toThrow();
    });

    it('should not throw if base api is created but opConfig has index set to another value', async () => {
        const job = newTestJobConfig({
            apis: [
                {
                    _name: 'spaces_reader_api',
                    index: 'test_index',
                    date_field_name: 'created',
                    endpoint: '127.0.0.1',
                    token: 'someToken'
                }
            ],
            operations: [
                {
                    _op: name,
                    index,
                    date_field_name: 'created',
                    endpoint: '127.0.0.1',
                    token: 'someToken'
                },
                { _op: 'noop' }
            ]
        });

        harness = new WorkerTestHarness(job, { clients });

        await harness.initialize();

        const apiConfig = harness.executionContext.config.apis.find(
            (api: APIConfig) => api._name === 'spaces_reader_api:spaces_reader-0'
        );

        expect(apiConfig).toMatchObject({ index });
    });
});
