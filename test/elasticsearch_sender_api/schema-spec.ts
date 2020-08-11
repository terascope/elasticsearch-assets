import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { AnyObject } from '@terascope/job-components';
import { getESVersion } from 'elasticsearch-store';
import { TEST_INDEX_PREFIX, makeClient } from '../helpers';
import { ElasticsearchSenderApi, DEFAULT_API_NAME } from '../../asset/src/elasticsearch_sender_api/interfaces';

describe('elasticsearch sender api schema', () => {
    const apiSenderIndex = `${TEST_INDEX_PREFIX}_elasticsearch_sender_api_schema_`;
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

    async function makeSchema(config: AnyObject = {}): Promise<ElasticsearchSenderApi> {
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
        return apis.find((settings) => settings._name === apiName) as ElasticsearchSenderApi;
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
        expect(type).toEqual('_doc');
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
