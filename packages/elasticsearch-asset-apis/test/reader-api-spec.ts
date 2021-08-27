import 'jest-extended';
import { getESVersion } from 'elasticsearch-store';
import { LATEST_VERSION, DataTypeConfig } from '@terascope/data-types';
import { debugLogger, DataEntity, toNumber } from '@terascope/utils';
import { DataFrame } from '@terascope/data-mate';
import { EventEmitter } from 'events';
import {
    TEST_INDEX_PREFIX,
    makeClient,
    cleanupIndex,
    populateIndex,
    waitForData,
    ELASTICSEARCH_VERSION,
} from './helpers';
import evenSpread from './fixtures/data/even-spread';
import {
    createElasticsearchReaderAPI,
    ElasticsearchReaderClient,
    ESReaderOptions,
    FetchResponseType,
    IDType,
    InputDateSegments,
    ReaderSlice
} from '../src';

describe('Reader API', () => {
    const client = makeClient();
    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_api_dataframe_`;
    const logger = debugLogger('api-dataFrame-test');
    const emitter = new EventEmitter();
    const idFieldName = 'uuid';

    function makeIndex(str: string): string {
        return `${readerIndex}_${str}`;
    }

    const evenIndex = makeIndex(evenSpread.index);

    const readerClient = new ElasticsearchReaderClient(
        client,
        { index: evenIndex },
        logger,
    );

    const evenBulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    const version = getESVersion(client);
    const docType = version === 5 ? 'events' : '_doc';

    beforeAll(async () => {
        await cleanupIndex(client, makeIndex('*'));
        await populateIndex(client, evenIndex, evenSpread.types, evenBulkData, docType);
        await waitForData(client, evenIndex, evenBulkData.length);
    });

    afterAll(async () => {
        await cleanupIndex(client, makeIndex('*'));
    });

    const typeConfig: DataTypeConfig = {
        version: LATEST_VERSION,
        fields: evenSpread.types
    };

    describe('returning data frames', () => {
        const defaultConfig: ESReaderOptions = Object.seal({
            index: evenIndex,
            size: 1000,
            date_field_name: 'created',
            query: '*',
            type: docType,
            response_type: FetchResponseType.data_frame,
            type_config: typeConfig,
            start: null,
            end: null,
            interval: 'auto',
            subslice_by_key: false,
            fields: [],
            delay: '1000s',
            subslice_key_threshold: 1000000,
            key_type: IDType.base64url,
            time_resolution: 'ms',
            connection: 'default',
            starting_key_depth: 0
        });

        it('can determine date ranges', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const results = await api.determineDateRanges();

            expect(results.start?.toISOString()).toEqual('2019-04-26T15:00:23.201Z');
            expect(results.limit?.toISOString()).toEqual('2019-04-26T15:00:23.394Z');
        });

        it('can determine slice interval', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const dates = await api.determineDateRanges() as InputDateSegments;
            const result = await api.determineSliceInterval(defaultConfig.interval, dates);

            expect(result).toEqual({ interval: [193, 'ms'], count: 1000 });
        });

        it('can make date slices', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const slicer = await api.makeDateSlicer({
                lifecycle: 'once',
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            const slice = await slicer() as ReaderSlice;

            expect(slice).toEqual({
                start: '2019-04-26T15:00:23.201Z',
                end: '2019-04-26T15:00:23.394Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 1000
            });

            expect(await slicer()).toBeNull();
        });

        it('can handle the case where no data is returned from the query', async () => {
            const config: ESReaderOptions = Object.freeze({
                ...defaultConfig,
                // there should be nothing with this range
                start: '2001-01-31T17:23:25.000Z',
                end: '2001-01-31T17:23:26.000Z'
            });

            const api = createElasticsearchReaderAPI({
                config, client: readerClient, logger, emitter
            });

            const slicer = await api.makeDateSlicer({
                lifecycle: 'once',
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            expect(await slicer()).toBeNull();
        });

        it('can count a slice', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });
            // query is set to * above
            const count = await api.count();

            expect(count).toEqual(1000);
        });

        it('can fetch records', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const results = await api.fetch() as DataFrame;

            expect(results).toBeInstanceOf(DataFrame);
            expect(results.size).toEqual(1000);
            expect(results.metadata.metrics).toBeObject();
        });

        it('can getWindowSize', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const size = await api.getWindowSize();

            expect(size).toBeNumber();
        });

        it('can get api version', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = createElasticsearchReaderAPI({
                config, client: readerClient, logger, emitter
            });

            const parsedNumber = toNumber(ELASTICSEARCH_VERSION.split('.')[0]);
            expect(api.version).toEqual(parsedNumber);
        });

        // TODO this is badly named method, might need to change in the future
        it('can verify index', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            let warnMessage: undefined|string;

            const testLogger = {
                info: () => {},
                warn: (msg: string) => {
                    warnMessage = msg;
                    return null;
                }
            } as any;

            const api = createElasticsearchReaderAPI({
                config,
                client: new ElasticsearchReaderClient(
                    client,
                    { index: evenIndex },
                    testLogger
                ),
                logger: testLogger,
                emitter
            });

            await api.verifyIndex();

            expect(warnMessage).toBeString();
        });

        it('can make id slices', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig,
                id_field_name: idFieldName,
                starting_key_depth: 0,
                key_type: IDType.base64url,
            };

            const api = createElasticsearchReaderAPI({
                config, client: readerClient, logger, emitter
            });

            const slicer = await api.makeIDSlicer({
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            const slice = await slicer() as ReaderSlice;

            expect(slice).toMatchObject({
                keys: ['a']
            });
        });
    });

    describe('returning raw data frames', () => {
        const defaultConfig: ESReaderOptions = Object.seal({
            index: evenIndex,
            size: 1000,
            date_field_name: 'created',
            query: '*',
            type: docType,
            response_type: FetchResponseType.raw,
            type_config: typeConfig,
            start: null,
            end: null,
            interval: 'auto',
            subslice_by_key: false,
            fields: [],
            delay: '1000s',
            subslice_key_threshold: 1000000,
            key_type: IDType.base64url,
            time_resolution: 'ms',
            connection: 'default',
            starting_key_depth: 0
        });

        it('can determine date ranges', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const results = await api.determineDateRanges();

            expect(results.start?.toISOString()).toEqual('2019-04-26T15:00:23.201Z');
            expect(results.limit?.toISOString()).toEqual('2019-04-26T15:00:23.394Z');
        });

        it('can determine slice interval', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const dates = await api.determineDateRanges() as InputDateSegments;
            const result = await api.determineSliceInterval(defaultConfig.interval, dates);

            expect(result).toEqual({ interval: [193, 'ms'], count: 1000 });
        });

        it('can make date slices', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const slicer = await api.makeDateSlicer({
                lifecycle: 'once',
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            const slice = await slicer() as ReaderSlice;

            expect(slice).toEqual({
                start: '2019-04-26T15:00:23.201Z',
                end: '2019-04-26T15:00:23.394Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 1000
            });

            expect(await slicer()).toBeNull();
        });

        it('can handle the case where no data is returned from the query', async () => {
            const config: ESReaderOptions = Object.freeze({
                ...defaultConfig,
                // there should be nothing with this range
                start: '2001-01-31T17:23:25.000Z',
                end: '2001-01-31T17:23:26.000Z'
            });

            const api = createElasticsearchReaderAPI({
                config, client: readerClient, logger, emitter
            });

            const slicer = await api.makeDateSlicer({
                lifecycle: 'once',
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            expect(await slicer()).toBeNull();
        });

        it('can count a slice', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });
            // query is set to * above
            const count = await api.count();

            expect(count).toEqual(1000);
        });

        it('can fetch records', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const results = await api.fetch() as DataFrame;

            expect(results).toBeInstanceOf(Buffer);
        });

        it('can getWindowSize', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const size = await api.getWindowSize();

            expect(size).toBeNumber();
        });

        it('can get api version', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = createElasticsearchReaderAPI({
                config, client: readerClient, logger, emitter
            });

            const parsedNumber = toNumber(ELASTICSEARCH_VERSION.split('.')[0]);
            expect(api.version).toEqual(parsedNumber);
        });

        // TODO this is badly named method, might need to change in the future
        it('can verify index', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            let warnMessage: undefined|string;

            const testLogger = {
                info: () => {},
                warn: (msg: string) => {
                    warnMessage = msg;
                    return null;
                }
            } as any;

            const api = createElasticsearchReaderAPI({
                config,
                client: new ElasticsearchReaderClient(
                    client,
                    { index: evenIndex },
                    testLogger
                ),
                logger: testLogger,
                emitter
            });

            await api.verifyIndex();

            expect(warnMessage).toBeString();
        });

        it('can make id slices', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig,
                id_field_name: idFieldName,
                starting_key_depth: 0,
                key_type: IDType.base64url,
            };

            const api = createElasticsearchReaderAPI({
                config, client: readerClient, logger, emitter
            });

            const slicer = await api.makeIDSlicer({
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            const slice = await slicer() as ReaderSlice;

            expect(slice).toMatchObject({
                keys: ['a']
            });
        });
    });

    describe('returning data entities', () => {
        const defaultConfig: ESReaderOptions = Object.freeze({
            index: evenIndex,
            size: 1000,
            date_field_name: 'created',
            query: '*',
            type: docType,
            response_type: FetchResponseType.data_entities,
            type_config: typeConfig,
            start: null,
            end: null,
            interval: 'auto',
            subslice_by_key: false,
            fields: [],
            delay: '1000s',
            subslice_key_threshold: 1000000,
            key_type: IDType.base64url,
            id_field_name: idFieldName,
            time_resolution: 'ms',
            connection: 'default',
            starting_key_depth: 0
        });

        it('can determine date ranges', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const results = await api.determineDateRanges();

            expect(results.start?.toISOString()).toEqual('2019-04-26T15:00:23.201Z');
            expect(results.limit?.toISOString()).toEqual('2019-04-26T15:00:23.394Z');
        });

        it('can determine slice interval', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const dates = await api.determineDateRanges() as InputDateSegments;
            const results = await api.determineSliceInterval(defaultConfig.interval, dates);

            expect(results).toEqual({
                interval: [193, 'ms'],
                count: 1000
            });
        });

        it('can make date slices', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const slicer = await api.makeDateSlicer({
                lifecycle: 'once',
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            const slice = await slicer() as ReaderSlice;

            expect(slice).toEqual({
                start: '2019-04-26T15:00:23.201Z',
                end: '2019-04-26T15:00:23.394Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 1000
            });

            expect(await slicer()).toBeNull();
        });

        it('can count a slice', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });
            // query is set to * above
            const count = await api.count();

            expect(count).toEqual(1000);
        });

        it('can fetch records', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const results = await api.fetch() as DataEntity[];

            expect(results).toBeArrayOfSize(1000);

            const [record] = results;

            expect(DataEntity.isDataEntity(record)).toBeTrue();

            const metadata = record.getMetadata();

            const {
                _type, _index, _key, _createTime
            } = metadata;

            expect(_type).toEqual(docType);
            expect(_index).toEqual(evenIndex);
            expect(_key).toBeString();
            expect(_createTime).toBeNumber();
        });

        it('can getWindowSize', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const size = await api.getWindowSize();

            expect(size).toBeNumber();
        });

        it('can get api version', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const parsedNumber = toNumber(ELASTICSEARCH_VERSION.split('.')[0]);
            expect(api.version).toEqual(parsedNumber);
        });

        // TODO this is badly named method, might need to change in the future
        it('can verify index', async () => {
            let warnMessage: undefined|string;

            const testLogger = {
                info: () => {},
                warn: (msg: string) => {
                    warnMessage = msg;
                    return null;
                }
            } as any;

            const api = createElasticsearchReaderAPI({
                config: defaultConfig,
                client: new ElasticsearchReaderClient(
                    client,
                    { index: evenIndex },
                    testLogger
                ),
                logger: testLogger,
                emitter
            });

            await api.verifyIndex();

            expect(warnMessage).toBeString();
        });

        it('can make id slices', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const slicer = await api.makeIDSlicer({
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            const slice = await slicer() as ReaderSlice;

            expect(slice).toEqual({
                keys: ['a'],
                count: 58
            });
        });

        it('will throw is size is beyond window_size of index', async () => {
            const size = 1000000000;

            const config: ESReaderOptions = Object.freeze({
                ...defaultConfig,
                size
            });

            const errMsg = `Invalid parameter size: ${size}, it cannot exceed the "index.max_result_window" index setting of 10000 for index ${config.index}`;

            try {
                const api = createElasticsearchReaderAPI({
                    config, client: readerClient, logger, emitter
                });

                await api.fetch({});
                throw new Error('should have error');
            } catch (err) {
                expect(
                    // @ts-expect-error
                    err.message
                ).toEqual(errMsg);
            }
        });
    });
});
