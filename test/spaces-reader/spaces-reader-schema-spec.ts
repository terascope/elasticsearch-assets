import 'jest-extended';
import {
    newTestJobConfig, AnyObject
} from '@terascope/job-components';
import { WorkerTestHarness } from 'teraslice-test-harness';

describe('spaces-reader schema', () => {
    let harness: WorkerTestHarness;

    async function makeTest(opConfig: AnyObject = {}, apiConfig?: AnyObject) {
        const readerConfig = Object.assign({
            _op: 'simple_api_reader',
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
            const newApiConfig = Object.assign({ _name: 'spaces_api_reader' }, apiConfig);
            partialJob.apis.push(newApiConfig);
        }

        harness = new WorkerTestHarness(newTestJobConfig(partialJob), {});

        await harness.initialize();

        return harness;
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    it('can properly instantiate and has defaults', async () => {
        const test = await makeTest({
            index: 'test_index',
            date_field_name: 'created',
            endpoint: '127.0.0.1',
            token: 'someToken'
        });

        const op = test.getOperation('simple_api_reader');
        const { api_name, connection } = op.opConfig;

        expect(connection).toEqual('default');
        expect(api_name).toEqual('spaces_reader_api');
    });

    it('will not throw if parameters are in api', async () => {
        const apiConfig = {
            index: 'test_index',
            date_field_name: 'created',
            endpoint: '127.0.0.1',
            token: 'someToken'
        };

        expect(async () => makeTest({}, apiConfig)).toResolve();
    });

    it('will throw if parameters are in both api and opConfig', async () => {
        const opConfig = {
            index: 'test_index',
            date_field_name: 'created',
            endpoint: '127.0.0.1',
            token: 'someToken'
        };

        const apiConfig = {
            index: 'test_index',
            date_field_name: 'created',
            endpoint: '127.0.0.1',
            token: 'someToken'
        };

        expect(async () => makeTest(opConfig, apiConfig)).toReject();
    });
});
