import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { AnyObject } from '@terascope/job-components';
import { getESVersion } from 'elasticsearch-store';
import { TEST_INDEX_PREFIX, makeClient, ELASTICSEARCH_VERSION } from '../helpers';
import { ElasticsearchReaderAPIConfig, DEFAULT_API_NAME } from '../../asset/src/elasticsearch_reader_api/interfaces';

describe('elasticsearch reader api schema', () => {
    const apiSenderIndex = `${TEST_INDEX_PREFIX}_elasticsearch_reader_api_schema_`;
    const esClient = makeClient();
    const version = getESVersion(esClient);
    const docType = version === 5 ? 'events' : '_doc';

    let harness: WorkerTestHarness;

    const clients = [
        {
            type: 'elasticsearch',
            endpoint: 'default',
            create: () => ({
                client: esClient
            }),
            config: {
                apiVersion: ELASTICSEARCH_VERSION
            }
        }
    ];

    const apiName = DEFAULT_API_NAME;
    const harnesses: WorkerTestHarness[] = [];

    async function makeSchema(config: AnyObject = {}): Promise<ElasticsearchReaderAPIConfig> {
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
        harnesses.push(harness);

        const { apis } = harness.executionContext.config;
        return apis.find((settings) => settings._name === apiName) as ElasticsearchReaderAPIConfig;
    }

    afterEach(async () => {
        await Promise.all(harnesses.map((testHarness) => testHarness.shutdown()));
    });

    it('should have defaults', async () => {
        const { connection } = await makeSchema({ index: apiSenderIndex });

        expect(connection).toEqual('default');
    });

    it('should values are incorrect', async () => {
        await expect(makeSchema({ connection: -4 })).toReject();
        await expect(makeSchema({ index: -3 })).toReject();
        await expect(makeSchema({ index: undefined })).toReject();
    });

    it('subslice_by_key configuration validation', async () => {
        const badOP = { subslice_by_key: true, type: undefined };
        const goodOP = { subslice_by_key: true, field: 'events-', type: docType };
        const otherGoodOP = { subslice_by_key: false, other: 'events-' };
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
        if (version <= 5) {
            await expect(makeSchema({ subslice_by_key: true, type: undefined })).toReject();
            await expect(makeSchema({ subslice_by_key: true, type: docType })).toResolve();
        } else {
            await expect(makeSchema({ subslice_by_key: true })).toReject();
            await expect(makeSchema({ subslice_by_key: true, field: 'hello' })).toResolve();
        }
    });
});
