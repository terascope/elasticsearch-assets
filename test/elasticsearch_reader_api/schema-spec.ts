import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { AnyObject } from '@terascope/job-components';
import { getESVersion } from 'elasticsearch-store';
import { TEST_INDEX_PREFIX, makeClient } from '../helpers';
import { ElasticsearchReaderConfig, DEFAULT_API_NAME } from '../../asset/src/elasticsearch_reader_api/interfaces';

describe('elasticsearch reader api schema', () => {
    const apiSenderIndex = `${TEST_INDEX_PREFIX}_elasticsearch_reader_api_schema_`;
    const esClient = makeClient();
    const version = getESVersion(esClient);
    const docType = version === 5 ? 'type' : '_doc';

    let harness: WorkerTestHarness;

    const clients = [
        {
            type: 'elasticsearch',
            endpoint: 'default',
            create: () => ({
                client: esClient
            }),
        }
    ];

    const apiName = DEFAULT_API_NAME;

    async function makeSchema(config: AnyObject = {}): Promise<ElasticsearchReaderConfig> {
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
        return apis.find((settings) => settings._name === apiName) as ElasticsearchReaderConfig;
    }

    afterEach(async () => {
        await harness.shutdown();
    });

    it('should have defaults', async () => {
        const { connection } = await makeSchema({ index: apiSenderIndex });

        expect(connection).toEqual('default');
    });

    it('should values are incorrect', async () => {
        await expect(makeSchema({ connection: -4 })).toReject();
        await expect(makeSchema({ index: -3 })).toReject();
    });
});
