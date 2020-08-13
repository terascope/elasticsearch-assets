import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { AnyObject } from '@terascope/job-components';
import { getESVersion } from 'elasticsearch-store';
import { TEST_INDEX_PREFIX, makeClient } from '../helpers';
import { ESStateStorageConfig } from '../../asset/src/elasticsearch_state_storage/interfaces';

describe('elasticsearch state storage api schema', () => {
    const apiReaderIndex = `${TEST_INDEX_PREFIX}_state__storage_api_`;
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

    const apiName = 'elasticsearch_state_storage';

    async function makeSchema(config: AnyObject = {}): Promise<ESStateStorageConfig> {
        const base: AnyObject = {
            _name: apiName,
            index: apiReaderIndex,
            type: docType,
            cache_size: 100000,
        };
        if (version <= 5) base.type = docType;
        const apiConfig = Object.assign({ _name: apiName }, base, config);

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
                },

            ],
        });
        harness = new WorkerTestHarness(job, { clients });

        await harness.initialize();

        const { apis } = harness.executionContext.config;
        return apis.find((settings) => settings._name === apiName) as ESStateStorageConfig;
    }

    afterEach(async () => {
        await harness.shutdown();
    });

    it('has defaults', async () => {
        const schema = await makeSchema();

        expect(schema).toMatchObject({
            type: docType,
            concurrency: 10,
            source_fields: [],
            chunk_size: 2500,
            persist: false,
            meta_key_field: '_key',
            connection: 'default',
            cache_size: 2147483647
        });
    });

    it('will throw if index is not set', async () => {
        await expect(makeSchema({ index: undefined })).toReject();
    });

    it('will throw other settings are not correct', async () => {
        await expect(makeSchema({ index: undefined })).toReject();
        await expect(makeSchema({ type: 34 })).toReject();
        await expect(makeSchema({ concurrency: -34 })).toReject();
        await expect(makeSchema({ concurrency: 'hello' })).toReject();
        await expect(makeSchema({ source_fields: 'hello' })).toReject();
        await expect(makeSchema({ source_fields: ['hello', 3] })).toReject();
        await expect(makeSchema({ chunk_size: -34 })).toReject();
        await expect(makeSchema({ chunk_size: 'stuff' })).toReject();
        await expect(makeSchema({ meta_key_field: 3453 })).toReject();
        await expect(makeSchema({ connection: 3453 })).toReject();
        await expect(makeSchema({ cache_size: -3453 })).toReject();
        await expect(makeSchema({ cache_size: 'field' })).toReject();
    });
});
