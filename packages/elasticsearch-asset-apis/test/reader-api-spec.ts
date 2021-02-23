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
    formatWildcardQuery
} from './helpers';
import evenSpread from './fixtures/data/even-spread';
import {
    createElasticsearchReaderAPI,
    ESReaderOptions,
    IDType,
    InputDateSegments,
    SlicerDateResults
} from '../src';

describe('Reader API', () => {
    const client = makeClient();
    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_api_dataframe_`;
    const logger = debugLogger('api-dataFrame-test');
    const emitter = new EventEmitter();
    const idFieldName = 'uuid';

    function makeIndex(str: string) {
        return `${readerIndex}_${str}`;
    }

    const evenIndex = makeIndex(evenSpread.index);
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
            use_data_frames: true,
            type_config: typeConfig,
            start: null,
            end: null,
            interval: 'auto',
            preserve_id: false,
            subslice_by_key: false,
            fields: [],
            delay: '1000s',
            subslice_key_threshold: 1000000,
            key_type: IDType.base64url,
            time_resolution: 'ms',
            connection: 'default',
            starting_key_depth: 1
        });

        it('can determine date ranges', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const results = await api.determineDateRanges();

            expect(results).toBeDefined();
            expect(results.start).toBeDefined();
            expect(results.limit).toBeDefined();

            expect(results.start?.toISOString()).toEqual('2019-04-26T15:00:23.201Z');
            expect(results.limit?.toISOString()).toEqual('2019-04-26T15:00:23.394Z');
        });

        it('can determine slice interval', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const dates = await api.determineDateRanges() as InputDateSegments;
            const results = await api.determineSliceInterval(config.interval, dates);

            expect(results).toBeDefined();
            expect(results).toBeArrayOfSize(2);
            expect(results).toEqual([193, 'ms']);
        });

        it('can make date slices', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const slicer = await api.makeDateSlicer({
                lifecycle: 'once',
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            expect(slicer).toBeDefined();

            const slice = await slicer() as SlicerDateResults;

            expect(slice).toBeDefined();
            expect(slice.start).toEqual('2019-04-26T15:00:23.201Z');
            expect(slice.end).toEqual('2019-04-26T15:00:23.394Z');
            expect(slice.limit).toEqual('2019-04-26T15:00:23.394Z');
            expect(slice.holes).toEqual([]);
            expect(slice.count).toEqual(1000);

            const nullSlice = await slicer();
            expect(nullSlice).toBeNull();
        });

        it('can count a slice', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });
            // query is set to * above
            const count = await api.count();

            expect(count).toEqual(1000);
        });

        it('can fetch records', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const results = await api.fetch() as DataFrame;

            expect(results).toBeInstanceOf(DataFrame);
            expect(results.size).toEqual(1000);
            expect(results.metadata.metrics).toBeDefined();
        });

        it('can getWindowSize', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const size = await api.getWindowSize();

            expect(size).toBeNumber();
        });

        it('can get api version', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
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

            const api = await createElasticsearchReaderAPI({
                config, client, logger: testLogger, emitter
            });

            await api.verifyIndex();

            expect(warnMessage).toBeDefined();
            expect(warnMessage).toBeString();
        });

        it('can make id slices', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const slicer = await api.makeIDSlicer({
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
                keyType: IDType.base64url,
                startingKeyDepth: 0,
                idFieldName,
            });

            expect(slicer).toBeDefined();

            const slice = await slicer() as SlicerDateResults;

            expect(slice).toBeDefined();

            const expectedResults = formatWildcardQuery([
                { key: 'a*', count: 58 },
            ], version, docType, idFieldName);

            [slice].forEach((result, index) => {
                expect(result).toMatchObject(expectedResults[index]);
            });
        });
    });

    describe('returning data entities', () => {
        const defaultConfig: ESReaderOptions = Object.seal({
            index: evenIndex,
            size: 1000,
            date_field_name: 'created',
            query: '*',
            type: docType,
            use_data_frames: false,
            type_config: typeConfig,
            start: null,
            end: null,
            interval: 'auto',
            preserve_id: false,
            subslice_by_key: false,
            fields: [],
            delay: '1000s',
            subslice_key_threshold: 1000000,
            key_type: IDType.base64url,
            time_resolution: 'ms',
            connection: 'default',
            starting_key_depth: 1
        });

        it('can determine date ranges', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const results = await api.determineDateRanges();

            expect(results).toBeDefined();
            expect(results.start).toBeDefined();
            expect(results.limit).toBeDefined();

            expect(results.start?.toISOString()).toEqual('2019-04-26T15:00:23.201Z');
            expect(results.limit?.toISOString()).toEqual('2019-04-26T15:00:23.394Z');
        });

        it('can determine slice interval', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const dates = await api.determineDateRanges() as InputDateSegments;
            const results = await api.determineSliceInterval(config.interval, dates);

            expect(results).toBeDefined();
            expect(results).toBeArrayOfSize(2);
            expect(results).toEqual([193, 'ms']);
        });

        it('can make date slices', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const slicer = await api.makeDateSlicer({
                lifecycle: 'once',
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            expect(slicer).toBeDefined();

            const slice = await slicer() as SlicerDateResults;

            expect(slice).toBeDefined();
            expect(slice.start).toEqual('2019-04-26T15:00:23.201Z');
            expect(slice.end).toEqual('2019-04-26T15:00:23.394Z');
            expect(slice.limit).toEqual('2019-04-26T15:00:23.394Z');
            expect(slice.holes).toEqual([]);
            expect(slice.count).toEqual(1000);

            const nullSlice = await slicer();
            expect(nullSlice).toBeNull();
        });

        it('can count a slice', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });
            // query is set to * above
            const count = await api.count();

            expect(count).toEqual(1000);
        });

        it('can fetch records', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const results = await api.fetch() as DataEntity[];

            expect(results).toBeDefined();
            expect(results.length).toEqual(1000);

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
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const size = await api.getWindowSize();

            expect(size).toBeNumber();
        });

        it('can get api version', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
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

            const api = await createElasticsearchReaderAPI({
                config, client, logger: testLogger, emitter
            });

            await api.verifyIndex();

            expect(warnMessage).toBeDefined();
            expect(warnMessage).toBeString();
        });

        it('can make id slices', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client, logger, emitter
            });

            const slicer = await api.makeIDSlicer({
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
                keyType: IDType.base64url,
                startingKeyDepth: 0,
                idFieldName
            });

            expect(slicer).toBeDefined();

            const slice = await slicer() as SlicerDateResults;

            const expectedResults = formatWildcardQuery([
                { key: 'a*', count: 58 },
            ], version, docType, idFieldName);

            [slice].forEach((result, index) => {
                expect(result).toMatchObject(expectedResults[index]);
            });
        });

        it('will throw is size is beyond window_size of index', async () => {
            const size = 1000000000;

            const config: ESReaderOptions = {
                ...defaultConfig,
                size
            };

            const errMsg = `Invalid parameter size: ${size}, it cannot exceed the "index.max_result_window" index setting of 10000 for index ${config.index}`;

            try {
                await createElasticsearchReaderAPI({
                    config, client, logger, emitter
                });
                throw new Error('should have error');
            } catch (err) {
                expect(err.message).toEqual(errMsg);
            }
        });
    });
});
