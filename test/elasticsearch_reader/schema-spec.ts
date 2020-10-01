import 'jest-extended';
import { AnyObject, newTestJobConfig } from '@terascope/job-components';
import { getESVersion } from 'elasticsearch-store';
import { WorkerTestHarness } from 'teraslice-test-harness';
import { ESReaderConfig } from '../../asset/src/elasticsearch_reader/interfaces';
import * as ESReaderSchema from '../../asset/src/elasticsearch_reader/schema';
import { makeClient, ELASTICSEARCH_VERSION } from '../helpers';
import { DEFAULT_API_NAME } from '../../asset/src/elasticsearch_reader_api/interfaces';

describe('elasticsearch_reader schema', () => {
    const index = 'some_index';
    const name = 'elasticsearch_reader';

    const esClient = makeClient();

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

    let harness: WorkerTestHarness;

    afterEach(async () => {
        if (harness) {
            harness.events.emit('worker:shutdown');
            await harness.shutdown();
        }
    });

    const version = getESVersion(esClient);

    const docType = version === 5 ? 'events' : '_doc';

    async function makeSchema(config: AnyObject = {}): Promise<ESReaderConfig> {
        const opConfig = Object.assign({}, { _op: name, index, date_field_name: 'created' }, config);
        harness = WorkerTestHarness.testFetcher(opConfig, { clients });

        await harness.initialize();

        const validConfig = harness.executionContext.config.operations.find(
            (testConfig) => testConfig._op === name
        );

        return validConfig as ESReaderConfig;
    }

    it('has defaults', async () => {
        const schema = await makeSchema();
        const {
            connection,
            size,
            interval,
            delay,
            subslice_by_key,
            time_resolution
        } = schema;

        expect(connection).toEqual('default');
        expect(size).toEqual(5000);
        expect(interval).toEqual('auto');
        expect(delay).toEqual('30s');
        expect(subslice_by_key).toBeFalse();
        expect(time_resolution).toEqual('s');
    });

    it('can geo validate', async () => {
        const geoPointValidation = ESReaderSchema.schema.geo_box_top_left.format;
        const validGeoDistance = ESReaderSchema.schema.geo_distance.format;
        const geoSortOrder = ESReaderSchema.schema.geo_sort_order.format;

        expect(() => geoPointValidation(null)).not.toThrowError();
        expect(() => validGeoDistance(null)).not.toThrowError();
        expect(() => geoSortOrder(null)).not.toThrowError();

        // @ts-expect-error
        expect(() => geoPointValidation(19.1234)).toThrowError('Invalid geo_point, must be a string IF specified');
        expect(() => geoPointValidation('19.1234')).toThrowError('Invalid geo_point, received 19.1234');
        expect(() => geoPointValidation('190.1234,85.2134')).toThrowError('Invalid latitude parameter, was given 190.1234, should be >= -90 and <= 90');
        expect(() => geoPointValidation('80.1234,185.2134')).toThrowError('Invalid longitude parameter, was given 185.2134, should be >= -180 and <= 180');
        expect(() => geoPointValidation('80.1234,-155.2134')).not.toThrowError();

        // @ts-expect-error
        expect(() => validGeoDistance(19.1234)).toThrowError('Invalid geo_distance parameter, must be a string IF specified');
        expect(() => validGeoDistance(' ')).toThrowError('Invalid geo_distance paramter, is formatted incorrectly');
        expect(() => validGeoDistance('200something')).toThrowError('Invalid unit type, did not have a proper unit of measurement (ie m, km, yd, ft)');
        expect(() => validGeoDistance('200km')).not.toThrowError();

        expect(() => geoSortOrder(1234)).toThrowError('Invalid geo_sort_order parameter, must be a string IF specified');
        expect(() => geoSortOrder('hello')).toThrowError('If geo_sort_order is specified it must be either "asc" or "desc"');
        expect(() => geoSortOrder('asc')).not.toThrowError();
    });

    it('subslice_by_key configuration validation', async () => {
        const badOP = { subslice_by_key: true };
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
        expect(goodOp).toBeDefined();

        const goodOp2 = await makeSchema(Object.assign({}, testOpConfig, otherGoodOP));
        expect(goodOp2).toBeDefined();
    });

    it('will throw if configured incorrectly', async () => {
        await expect(makeSchema({ api_name: [1, 2, 3] })).toReject();

        await expect(makeSchema({ index: [1, 2, 3] })).toReject();
        await expect(makeSchema({ index: 'Hello' })).toReject();

        await expect(makeSchema({ field: 1234 })).toReject();
        await expect(makeSchema({ size: -1234 })).toReject();
        await expect(makeSchema({ size: 'stuff' })).toReject();

        await expect(makeSchema({ start: 'stuff' })).toReject();
        await expect(makeSchema({ start: { other: 3 } })).toReject();

        await expect(makeSchema({ end: 'stuff' })).toReject();
        await expect(makeSchema({ end: { other: 3 } })).toReject();

        await expect(makeSchema({ interval: 'stuff' })).toReject();
        await expect(makeSchema({ interval: 23423 })).toReject();

        await expect(makeSchema({ date_field_name: 23423 })).toReject();
        await expect(makeSchema({ date_field_name: null })).toReject();

        await expect(makeSchema({ query: 38472 })).toReject();

        await expect(makeSchema({ fields: 'hello' })).toReject();
        await expect(makeSchema({ fields: ['hello', 39] })).toReject();

        await expect(makeSchema({ delay: ['hello', 39] })).toReject();
        await expect(makeSchema({ delay: 'hello' })).toReject();
        await expect(makeSchema({ delay: 23423 })).toReject();

        await expect(makeSchema({ subslice_key_threshold: -23423 })).toReject();

        await expect(makeSchema({ key_type: 23423 })).toReject();
        await expect(makeSchema({ key_type: 'something' })).toReject();

        await expect(makeSchema({ time_resolution: 'something' })).toReject();
    });

    it('should throw if in subslice_by_key is set but type is not in elasticsearch <= v5', async () => {
        if (version <= 5) {
            await expect(makeSchema({ subslice_by_key: true })).toReject();
            await expect(makeSchema({ subslice_by_key: true, type: docType })).toResolve();
        } else {
            await expect(makeSchema({ subslice_by_key: true })).toReject();
            await expect(makeSchema({ subslice_by_key: true, field: 'hello' })).toResolve();
        }
    });

    it('should throw if api is created but opConfig has index set to another value', async () => {
        const job = newTestJobConfig({
            apis: [
                {
                    _name: DEFAULT_API_NAME,
                    index,
                    type: docType,
                    date_field_name: 'created'
                }
            ],
            operations: [
                {
                    _op: name,
                    index: 'something_else',
                    api_name: DEFAULT_API_NAME,
                    date_field_name: 'created'
                },
                { _op: 'noop' }
            ]
        });

        expect(() => new WorkerTestHarness(job, { clients })).toThrow();
    });

    it('should not throw if base api is created but opConfig has index set to another value', async () => {
        const job = newTestJobConfig({
            apis: [
                {
                    _name: DEFAULT_API_NAME,
                    index,
                    type: docType,
                    date_field_name: 'created'
                }
            ],
            operations: [
                { _op: name, index, date_field_name: 'created' },
                { _op: 'noop' }
            ]
        });

        harness = new WorkerTestHarness(job, { clients });

        await harness.initialize();

        const apiConfig = harness.executionContext.config.apis.find(
            (api) => api._name === 'elasticsearch_reader_api:elasticsearch_reader-0'
        );

        expect(apiConfig).toBeDefined();
        expect(apiConfig!.index).toEqual(index);
    });
});
