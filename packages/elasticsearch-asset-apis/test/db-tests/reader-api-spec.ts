import 'jest-extended';
import { debugLogger, DataEntity, pWhile, pMap } from '@terascope/utils';
import {
    ElasticsearchTestHelpers, getClientMetadata, isOpensearch2,
    isElasticsearch8
} from 'elasticsearch-store';
import { DataFrame } from '@terascope/data-mate';
import { EventEmitter } from 'node:events';
import {
    TEST_INDEX_PREFIX, cleanupIndex, populateIndex,
    waitForData, makeClient
} from '../helpers/index.js';
import {
    createElasticsearchReaderAPI, DateSlicerRange, ElasticsearchReaderClient,
    ESReaderOptions, FetchResponseType, IDType,
    InputDateSegments, ReaderSlice
} from '../../src/index.js';

describe('Reader API', () => {
    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_api_dataframe_`;
    const logger = debugLogger('api-dataFrame-test');
    const emitter = new EventEmitter();
    const idFieldName = 'uuid';

    function makeIndex(str: string): string {
        return `${readerIndex}_${str}`;
    }

    async function gatherSlices(fn: () => Promise<any>) {
        const results: any[] = [];

        await pWhile(async () => {
            const slice = await fn();
            results.push(slice);

            if (slice == null) {
                return true;
            }
        }, { timeoutMs: 100000 });

        return results;
    }

    const evenSpread = ElasticsearchTestHelpers.EvenDateData;

    const evenIndex = makeIndex('even_spread');
    const evenBulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));
    let docType: string | undefined;

    let client: any;
    let readerClient: ElasticsearchReaderClient;
    let majorVersion: number;

    beforeAll(async () => {
        client = await makeClient();

        if (isOpensearch2(client) || isElasticsearch8(client)) {
            docType = undefined;
        } else {
            docType = '_doc';
        }

        const results = getClientMetadata(client);
        majorVersion = results.majorVersion;

        readerClient = new ElasticsearchReaderClient(
            client,
            { index: evenIndex },
            logger,
        );

        await cleanupIndex(client, makeIndex('*'));
        await populateIndex(client, evenIndex, evenSpread.EvenDataType, evenBulkData, docType);
        await waitForData(client, evenIndex, evenBulkData.length);
    });

    afterAll(async () => {
        await cleanupIndex(client, makeIndex('*'));
    });

    describe('returning data frames', () => {
        const defaultConfig: ESReaderOptions = Object.seal({
            index: evenIndex,
            size: 1000,
            date_field_name: 'created',
            query: '*',
            response_type: FetchResponseType.data_frame,
            type_config: evenSpread.EvenDataType,
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
            starting_key_depth: 0,
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

            expect(api.version).toEqual(majorVersion);
        });

        // TODO this is badly named method, might need to change in the future
        it('can verify index', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            let warnMessage: undefined | string;

            const testLogger = {
                info: () => {},
                warn: (msg: string) => {
                    warnMessage = msg;
                    return null;
                }
            } as any;

            const api = createElasticsearchReaderAPI({
                config,
                client: new ElasticsearchReaderClient(client, { index: evenIndex }, testLogger
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
            response_type: FetchResponseType.raw,
            type_config: evenSpread.EvenDataType,
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

            expect(api.version).toEqual(majorVersion);
        });

        // TODO this is badly named method, might need to change in the future
        it('can verify index', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            let warnMessage: undefined | string;

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
            response_type: FetchResponseType.data_entities,
            type_config: evenSpread.EvenDataType,
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

            expect(api.version).toEqual(majorVersion);
        });

        // TODO this is badly named method, might need to change in the future
        it('can verify index', async () => {
            let warnMessage: undefined | string;

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

        it('can make id slices with recurse_optimization', async () => {
            const opConfig = {
                ...defaultConfig,
                size: 40,
                recurse_optimization: true
            };

            const api = createElasticsearchReaderAPI({
                config: opConfig, client: readerClient, logger, emitter
            });

            const slicer = await api.makeIDSlicer({
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            const expectedSlices = [
                { keys: ['a[A-Za-r]'], count: 18 },
                { keys: ['a[s-z0-9-_]'], count: 40 },
                { keys: ['b[A-Za-e]'], count: 29 },
                { keys: ['b[f-z0-1]'], count: 9 },
                { keys: ['b[2-8]'], count: 37 },
                { keys: ['b[9-_]'], count: 7 },
                { keys: ['c[A-Za-n]'], count: 18 },
                { keys: ['c[o-z0-7]'], count: 40 },
                { keys: ['c[8-9-_]'], count: 6 },
                { keys: ['d[A-Za-z]'], count: 17 },
                { keys: ['d[0-9-_]'], count: 32 },
                { keys: ['e[A-Za-q]'], count: 18 },
                { keys: ['e[r-z0-8]'], count: 36 },
                { keys: ['e[9-_]'], count: 5 },
                { keys: ['f[A-Za-x]'], count: 21 },
                { keys: ['f[y-z0-9-_]'], count: 30 },
                { keys: ['0[A-Za-j]'], count: 33 },
                { keys: ['0[k-z0-9-_]'], count: 37 },
                { keys: ['1[A-Za-t]'], count: 25 },
                { keys: ['1[u-z0-9-_]'], count: 30 },
                { keys: ['2[A-Za-t]'], count: 21 },
                { keys: ['2[u-z0-9-_]'], count: 34 },
                { keys: ['3[A-Za-u]'], count: 25 },
                { keys: ['3[v-z0-9-_]'], count: 29 },
                { keys: ['4[A-Za-k]'], count: 25 },
                { keys: ['4[l-z0-8]'], count: 34 },
                { keys: ['4[9-_]'], count: 9 },
                { keys: ['5[A-Za-n]'], count: 22 },
                { keys: ['5[o-z0-8]'], count: 37 },
                { keys: ['5[9-_]'], count: 5 },
                { keys: ['6[A-Za-w]'], count: 24 },
                { keys: ['6[x-z0-9-_]'], count: 28 },
                { keys: ['7[A-Za-f]'], count: 35 },
                { keys: ['7[g-z0-7]'], count: 37 },
                { keys: ['7[8-9-_]'], count: 8 },
                { keys: ['8[A-Za-h]'], count: 17 },
                { keys: ['8[i-z0-4]'], count: 23 },
                { keys: ['8[5-9-_]'], count: 35 },
                { keys: ['9[A-Za-n]'], count: 23 },
                { keys: ['9[o-z0-8]'], count: 40 },
                { keys: ['9[9-_]'], count: 1 },
            ];

            const slices = await gatherSlices(slicer);

            // get rid of the null
            slices.pop();

            const sliceCount = slices.reduce((prev, curr) => {
                return curr.count + prev;
            }, 0);

            expect(sliceCount).toEqual(evenBulkData.length);
            expect(slices).toEqual(expectedSlices);

            const records = await pMap(slices, async (slice) => {
                const data = await api.fetch(slice) as DataEntity[];

                return { slice, data, count: data.length };
            });

            const recordCount = records.reduce((prev, curr) => {
                return curr.count + prev;
            }, 0);

            expect(recordCount).toEqual(evenBulkData.length);
        });

        it('can make id slices with recurse_optimization with base64 "+" symbol', async () => {
            const opConfig = {
                ...defaultConfig,
                key_type: IDType.base64,
                starting_key_depth: 1,
                size: 40,
                recurse_optimization: true
            };

            const api = createElasticsearchReaderAPI({
                config: opConfig, client: readerClient, logger, emitter
            });

            const slicer = await api.makeIDSlicer({
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            const expectedSlices = [
                { keys: ['aa'], count: 3 },
                { keys: ['ab'], count: 3 },
                { keys: ['ac'], count: 3 },
                { keys: ['ad'], count: 4 },
                { keys: ['ae'], count: 4 },
                { keys: ['af'], count: 1 },
                { keys: ['a0'], count: 5 },
                { keys: ['a1'], count: 7 },
                { keys: ['a3'], count: 2 },
                { keys: ['a4'], count: 3 },
                { keys: ['a5'], count: 3 },
                { keys: ['a6'], count: 3 },
                { keys: ['a7'], count: 4 },
                { keys: ['a8'], count: 5 },
                { keys: ['a9'], count: 8 },
                { keys: ['ba'], count: 7 },
                { keys: ['bb'], count: 2 },
                { keys: ['bc'], count: 6 },
                { keys: ['bd'], count: 8 },
                { keys: ['be'], count: 6 },
                { keys: ['bf'], count: 6 },
                { keys: ['b0'], count: 2 },
                { keys: ['b1'], count: 1 },
                { keys: ['b2'], count: 4 },
                { keys: ['b3'], count: 5 },
                { keys: ['b4'], count: 5 },
                { keys: ['b5'], count: 5 },
                { keys: ['b6'], count: 5 },
                { keys: ['b7'], count: 7 },
                { keys: ['b8'], count: 6 },
                { keys: ['b9'], count: 7 },
                { keys: ['ca'], count: 4 },
                { keys: ['cb'], count: 3 },
                { keys: ['cc'], count: 2 },
                { keys: ['cd'], count: 2 },
                { keys: ['ce'], count: 2 },
                { keys: ['cf'], count: 5 },
                { keys: ['c0'], count: 1 },
                { keys: ['c1'], count: 2 },
                { keys: ['c2'], count: 6 },
                { keys: ['c3'], count: 9 },
                { keys: ['c4'], count: 6 },
                { keys: ['c5'], count: 6 },
                { keys: ['c6'], count: 9 },
                { keys: ['c7'], count: 1 },
                { keys: ['c8'], count: 3 },
                { keys: ['c9'], count: 3 },
                { keys: ['da'], count: 1 },
                { keys: ['db'], count: 3 },
                { keys: ['dc'], count: 3 },
                { keys: ['dd'], count: 1 },
                { keys: ['de'], count: 4 },
                { keys: ['df'], count: 5 },
                { keys: ['d0'], count: 3 },
                { keys: ['d1'], count: 1 },
                { keys: ['d2'], count: 3 },
                { keys: ['d3'], count: 3 },
                { keys: ['d4'], count: 6 },
                { keys: ['d5'], count: 1 },
                { keys: ['d6'], count: 5 },
                { keys: ['d7'], count: 7 },
                { keys: ['d8'], count: 1 },
                { keys: ['d9'], count: 2 },
                { keys: ['ea'], count: 2 },
                { keys: ['eb'], count: 4 },
                { keys: ['ec'], count: 1 },
                { keys: ['ed'], count: 5 },
                { keys: ['ee'], count: 1 },
                { keys: ['ef'], count: 5 },
                { keys: ['e0'], count: 7 },
                { keys: ['e2'], count: 6 },
                { keys: ['e3'], count: 5 },
                { keys: ['e4'], count: 3 },
                { keys: ['e5'], count: 6 },
                { keys: ['e6'], count: 4 },
                { keys: ['e7'], count: 2 },
                { keys: ['e8'], count: 3 },
                { keys: ['e9'], count: 5 },
                { keys: ['fa'], count: 3 },
                { keys: ['fb'], count: 4 },
                { keys: ['fc'], count: 3 },
                { keys: ['fd'], count: 6 },
                { keys: ['fe'], count: 1 },
                { keys: ['ff'], count: 4 },
                { keys: ['f1'], count: 1 },
                { keys: ['f2'], count: 2 },
                { keys: ['f3'], count: 4 },
                { keys: ['f4'], count: 2 },
                { keys: ['f5'], count: 6 },
                { keys: ['f6'], count: 1 },
                { keys: ['f7'], count: 3 },
                { keys: ['f8'], count: 4 },
                { keys: ['f9'], count: 7 },
                { keys: ['0a'], count: 8 },
                { keys: ['0b'], count: 6 },
                { keys: ['0c'], count: 6 },
                { keys: ['0d'], count: 3 },
                { keys: ['0e'], count: 3 },
                { keys: ['0f'], count: 7 },
                { keys: ['00'], count: 8 },
                { keys: ['01'], count: 4 },
                { keys: ['02'], count: 3 },
                { keys: ['03'], count: 1 },
                { keys: ['04'], count: 5 },
                { keys: ['05'], count: 2 },
                { keys: ['06'], count: 4 },
                { keys: ['07'], count: 5 },
                { keys: ['08'], count: 1 },
                { keys: ['09'], count: 4 },
                { keys: ['1a'], count: 7 },
                { keys: ['1b'], count: 2 },
                { keys: ['1c'], count: 7 },
                { keys: ['1d'], count: 2 },
                { keys: ['1e'], count: 6 },
                { keys: ['1f'], count: 1 },
                { keys: ['10'], count: 2 },
                { keys: ['11'], count: 3 },
                { keys: ['12'], count: 6 },
                { keys: ['13'], count: 7 },
                { keys: ['14'], count: 4 },
                { keys: ['15'], count: 1 },
                { keys: ['16'], count: 2 },
                { keys: ['17'], count: 3 },
                { keys: ['18'], count: 1 },
                { keys: ['19'], count: 1 },
                { keys: ['2a'], count: 3 },
                { keys: ['2b'], count: 5 },
                { keys: ['2c'], count: 3 },
                { keys: ['2d'], count: 1 },
                { keys: ['2e'], count: 5 },
                { keys: ['2f'], count: 4 },
                { keys: ['20'], count: 2 },
                { keys: ['21'], count: 6 },
                { keys: ['22'], count: 2 },
                { keys: ['23'], count: 4 },
                { keys: ['24'], count: 6 },
                { keys: ['25'], count: 4 },
                { keys: ['26'], count: 2 },
                { keys: ['27'], count: 3 },
                { keys: ['28'], count: 2 },
                { keys: ['29'], count: 3 },
                { keys: ['3a'], count: 5 },
                { keys: ['3b'], count: 4 },
                { keys: ['3c'], count: 8 },
                { keys: ['3d'], count: 2 },
                { keys: ['3e'], count: 2 },
                { keys: ['3f'], count: 4 },
                { keys: ['31'], count: 2 },
                { keys: ['32'], count: 2 },
                { keys: ['34'], count: 1 },
                { keys: ['35'], count: 5 },
                { keys: ['36'], count: 4 },
                { keys: ['37'], count: 4 },
                { keys: ['38'], count: 4 },
                { keys: ['39'], count: 7 },
                { keys: ['4a'], count: 7 },
                { keys: ['4b'], count: 8 },
                { keys: ['4c'], count: 1 },
                { keys: ['4d'], count: 4 },
                { keys: ['4e'], count: 2 },
                { keys: ['4f'], count: 3 },
                { keys: ['40'], count: 7 },
                { keys: ['41'], count: 2 },
                { keys: ['42'], count: 4 },
                { keys: ['43'], count: 1 },
                { keys: ['44'], count: 4 },
                { keys: ['45'], count: 6 },
                { keys: ['46'], count: 4 },
                { keys: ['47'], count: 3 },
                { keys: ['48'], count: 3 },
                { keys: ['49'], count: 9 },
                { keys: ['5a'], count: 2 },
                { keys: ['5b'], count: 2 },
                { keys: ['5c'], count: 3 },
                { keys: ['5d'], count: 10 },
                { keys: ['5e'], count: 2 },
                { keys: ['5f'], count: 3 },
                { keys: ['50'], count: 4 },
                { keys: ['51'], count: 6 },
                { keys: ['52'], count: 3 },
                { keys: ['53'], count: 5 },
                { keys: ['54'], count: 4 },
                { keys: ['55'], count: 6 },
                { keys: ['56'], count: 3 },
                { keys: ['57'], count: 4 },
                { keys: ['58'], count: 2 },
                { keys: ['59'], count: 5 },
                { keys: ['6a'], count: 4 },
                { keys: ['6b'], count: 3 },
                { keys: ['6c'], count: 4 },
                { keys: ['6d'], count: 3 },
                { keys: ['6e'], count: 2 },
                { keys: ['6f'], count: 8 },
                { keys: ['60'], count: 2 },
                { keys: ['62'], count: 4 },
                { keys: ['63'], count: 5 },
                { keys: ['64'], count: 2 },
                { keys: ['65'], count: 1 },
                { keys: ['66'], count: 4 },
                { keys: ['67'], count: 1 },
                { keys: ['68'], count: 4 },
                { keys: ['69'], count: 5 },
                { keys: ['7a'], count: 7 },
                { keys: ['7b'], count: 7 },
                { keys: ['7c'], count: 5 },
                { keys: ['7d'], count: 4 },
                { keys: ['7e'], count: 8 },
                { keys: ['7f'], count: 4 },
                { keys: ['70'], count: 3 },
                { keys: ['71'], count: 4 },
                { keys: ['72'], count: 6 },
                { keys: ['73'], count: 4 },
                { keys: ['74'], count: 6 },
                { keys: ['75'], count: 6 },
                { keys: ['76'], count: 2 },
                { keys: ['77'], count: 6 },
                { keys: ['78'], count: 3 },
                { keys: ['79'], count: 5 },
                { keys: ['8a'], count: 2 },
                { keys: ['8b'], count: 1 },
                { keys: ['8c'], count: 4 },
                { keys: ['8d'], count: 2 },
                { keys: ['8e'], count: 4 },
                { keys: ['8f'], count: 4 },
                { keys: ['80'], count: 4 },
                { keys: ['81'], count: 3 },
                { keys: ['82'], count: 3 },
                { keys: ['83'], count: 7 },
                { keys: ['84'], count: 6 },
                { keys: ['85'], count: 4 },
                { keys: ['86'], count: 9 },
                { keys: ['87'], count: 7 },
                { keys: ['88'], count: 6 },
                { keys: ['89'], count: 9 },
                { keys: ['9a'], count: 5 },
                { keys: ['9b'], count: 6 },
                { keys: ['9c'], count: 4 },
                { keys: ['9d'], count: 3 },
                { keys: ['9e'], count: 1 },
                { keys: ['9f'], count: 4 },
                { keys: ['90'], count: 8 },
                { keys: ['91'], count: 3 },
                { keys: ['92'], count: 2 },
                { keys: ['93'], count: 10 },
                { keys: ['94'], count: 5 },
                { keys: ['95'], count: 1 },
                { keys: ['96'], count: 4 },
                { keys: ['97'], count: 5 },
                { keys: ['98'], count: 2 },
                { keys: ['99'], count: 1 },
            ];

            const slices = await gatherSlices(slicer);

            // get rid of the null
            slices.pop();

            const sliceCount = slices.reduce((prev, curr) => {
                return curr.count + prev;
            }, 0);

            expect(sliceCount).toEqual(evenBulkData.length);
            expect(slices).toEqual(expectedSlices);

            const records = await pMap(slices, async (slice) => {
                const data = await api.fetch(slice) as DataEntity[];

                return { slice, data, count: data.length };
            });

            const recordCount = records.reduce((prev, curr) => {
                return curr.count + prev;
            }, 0);

            expect(recordCount).toEqual(evenBulkData.length);
        });

        it('can make id slices with recurse_optimization with base64URL', async () => {
            const opConfig = {
                ...defaultConfig,
                key_type: IDType.base64url,
                starting_key_depth: 1,
                size: 40,
                recurse_optimization: true
            };

            const api = createElasticsearchReaderAPI({
                config: opConfig, client: readerClient, logger, emitter
            });

            const slicer = await api.makeIDSlicer({
                slicerID: 0,
                numOfSlicers: 1,
                recoveryData: [],
            });

            const expectedSlices = [
                { keys: ['aa'], count: 3 },
                { keys: ['ab'], count: 3 },
                { keys: ['ac'], count: 3 },
                { keys: ['ad'], count: 4 },
                { keys: ['ae'], count: 4 },
                { keys: ['af'], count: 1 },
                { keys: ['a0'], count: 5 },
                { keys: ['a1'], count: 7 },
                { keys: ['a3'], count: 2 },
                { keys: ['a4'], count: 3 },
                { keys: ['a5'], count: 3 },
                { keys: ['a6'], count: 3 },
                { keys: ['a7'], count: 4 },
                { keys: ['a8'], count: 5 },
                { keys: ['a9'], count: 8 },
                { keys: ['ba'], count: 7 },
                { keys: ['bb'], count: 2 },
                { keys: ['bc'], count: 6 },
                { keys: ['bd'], count: 8 },
                { keys: ['be'], count: 6 },
                { keys: ['bf'], count: 6 },
                { keys: ['b0'], count: 2 },
                { keys: ['b1'], count: 1 },
                { keys: ['b2'], count: 4 },
                { keys: ['b3'], count: 5 },
                { keys: ['b4'], count: 5 },
                { keys: ['b5'], count: 5 },
                { keys: ['b6'], count: 5 },
                { keys: ['b7'], count: 7 },
                { keys: ['b8'], count: 6 },
                { keys: ['b9'], count: 7 },
                { keys: ['ca'], count: 4 },
                { keys: ['cb'], count: 3 },
                { keys: ['cc'], count: 2 },
                { keys: ['cd'], count: 2 },
                { keys: ['ce'], count: 2 },
                { keys: ['cf'], count: 5 },
                { keys: ['c0'], count: 1 },
                { keys: ['c1'], count: 2 },
                { keys: ['c2'], count: 6 },
                { keys: ['c3'], count: 9 },
                { keys: ['c4'], count: 6 },
                { keys: ['c5'], count: 6 },
                { keys: ['c6'], count: 9 },
                { keys: ['c7'], count: 1 },
                { keys: ['c8'], count: 3 },
                { keys: ['c9'], count: 3 },
                { keys: ['da'], count: 1 },
                { keys: ['db'], count: 3 },
                { keys: ['dc'], count: 3 },
                { keys: ['dd'], count: 1 },
                { keys: ['de'], count: 4 },
                { keys: ['df'], count: 5 },
                { keys: ['d0'], count: 3 },
                { keys: ['d1'], count: 1 },
                { keys: ['d2'], count: 3 },
                { keys: ['d3'], count: 3 },
                { keys: ['d4'], count: 6 },
                { keys: ['d5'], count: 1 },
                { keys: ['d6'], count: 5 },
                { keys: ['d7'], count: 7 },
                { keys: ['d8'], count: 1 },
                { keys: ['d9'], count: 2 },
                { keys: ['ea'], count: 2 },
                { keys: ['eb'], count: 4 },
                { keys: ['ec'], count: 1 },
                { keys: ['ed'], count: 5 },
                { keys: ['ee'], count: 1 },
                { keys: ['ef'], count: 5 },
                { keys: ['e0'], count: 7 },
                { keys: ['e2'], count: 6 },
                { keys: ['e3'], count: 5 },
                { keys: ['e4'], count: 3 },
                { keys: ['e5'], count: 6 },
                { keys: ['e6'], count: 4 },
                { keys: ['e7'], count: 2 },
                { keys: ['e8'], count: 3 },
                { keys: ['e9'], count: 5 },
                { keys: ['fa'], count: 3 },
                { keys: ['fb'], count: 4 },
                { keys: ['fc'], count: 3 },
                { keys: ['fd'], count: 6 },
                { keys: ['fe'], count: 1 },
                { keys: ['ff'], count: 4 },
                { keys: ['f1'], count: 1 },
                { keys: ['f2'], count: 2 },
                { keys: ['f3'], count: 4 },
                { keys: ['f4'], count: 2 },
                { keys: ['f5'], count: 6 },
                { keys: ['f6'], count: 1 },
                { keys: ['f7'], count: 3 },
                { keys: ['f8'], count: 4 },
                { keys: ['f9'], count: 7 },
                { keys: ['0a'], count: 8 },
                { keys: ['0b'], count: 6 },
                { keys: ['0c'], count: 6 },
                { keys: ['0d'], count: 3 },
                { keys: ['0e'], count: 3 },
                { keys: ['0f'], count: 7 },
                { keys: ['00'], count: 8 },
                { keys: ['01'], count: 4 },
                { keys: ['02'], count: 3 },
                { keys: ['03'], count: 1 },
                { keys: ['04'], count: 5 },
                { keys: ['05'], count: 2 },
                { keys: ['06'], count: 4 },
                { keys: ['07'], count: 5 },
                { keys: ['08'], count: 1 },
                { keys: ['09'], count: 4 },
                { keys: ['1a'], count: 7 },
                { keys: ['1b'], count: 2 },
                { keys: ['1c'], count: 7 },
                { keys: ['1d'], count: 2 },
                { keys: ['1e'], count: 6 },
                { keys: ['1f'], count: 1 },
                { keys: ['10'], count: 2 },
                { keys: ['11'], count: 3 },
                { keys: ['12'], count: 6 },
                { keys: ['13'], count: 7 },
                { keys: ['14'], count: 4 },
                { keys: ['15'], count: 1 },
                { keys: ['16'], count: 2 },
                { keys: ['17'], count: 3 },
                { keys: ['18'], count: 1 },
                { keys: ['19'], count: 1 },
                { keys: ['2a'], count: 3 },
                { keys: ['2b'], count: 5 },
                { keys: ['2c'], count: 3 },
                { keys: ['2d'], count: 1 },
                { keys: ['2e'], count: 5 },
                { keys: ['2f'], count: 4 },
                { keys: ['20'], count: 2 },
                { keys: ['21'], count: 6 },
                { keys: ['22'], count: 2 },
                { keys: ['23'], count: 4 },
                { keys: ['24'], count: 6 },
                { keys: ['25'], count: 4 },
                { keys: ['26'], count: 2 },
                { keys: ['27'], count: 3 },
                { keys: ['28'], count: 2 },
                { keys: ['29'], count: 3 },
                { keys: ['3a'], count: 5 },
                { keys: ['3b'], count: 4 },
                { keys: ['3c'], count: 8 },
                { keys: ['3d'], count: 2 },
                { keys: ['3e'], count: 2 },
                { keys: ['3f'], count: 4 },
                { keys: ['31'], count: 2 },
                { keys: ['32'], count: 2 },
                { keys: ['34'], count: 1 },
                { keys: ['35'], count: 5 },
                { keys: ['36'], count: 4 },
                { keys: ['37'], count: 4 },
                { keys: ['38'], count: 4 },
                { keys: ['39'], count: 7 },
                { keys: ['4a'], count: 7 },
                { keys: ['4b'], count: 8 },
                { keys: ['4c'], count: 1 },
                { keys: ['4d'], count: 4 },
                { keys: ['4e'], count: 2 },
                { keys: ['4f'], count: 3 },
                { keys: ['40'], count: 7 },
                { keys: ['41'], count: 2 },
                { keys: ['42'], count: 4 },
                { keys: ['43'], count: 1 },
                { keys: ['44'], count: 4 },
                { keys: ['45'], count: 6 },
                { keys: ['46'], count: 4 },
                { keys: ['47'], count: 3 },
                { keys: ['48'], count: 3 },
                { keys: ['49'], count: 9 },
                { keys: ['5a'], count: 2 },
                { keys: ['5b'], count: 2 },
                { keys: ['5c'], count: 3 },
                { keys: ['5d'], count: 10 },
                { keys: ['5e'], count: 2 },
                { keys: ['5f'], count: 3 },
                { keys: ['50'], count: 4 },
                { keys: ['51'], count: 6 },
                { keys: ['52'], count: 3 },
                { keys: ['53'], count: 5 },
                { keys: ['54'], count: 4 },
                { keys: ['55'], count: 6 },
                { keys: ['56'], count: 3 },
                { keys: ['57'], count: 4 },
                { keys: ['58'], count: 2 },
                { keys: ['59'], count: 5 },
                { keys: ['6a'], count: 4 },
                { keys: ['6b'], count: 3 },
                { keys: ['6c'], count: 4 },
                { keys: ['6d'], count: 3 },
                { keys: ['6e'], count: 2 },
                { keys: ['6f'], count: 8 },
                { keys: ['60'], count: 2 },
                { keys: ['62'], count: 4 },
                { keys: ['63'], count: 5 },
                { keys: ['64'], count: 2 },
                { keys: ['65'], count: 1 },
                { keys: ['66'], count: 4 },
                { keys: ['67'], count: 1 },
                { keys: ['68'], count: 4 },
                { keys: ['69'], count: 5 },
                { keys: ['7a'], count: 7 },
                { keys: ['7b'], count: 7 },
                { keys: ['7c'], count: 5 },
                { keys: ['7d'], count: 4 },
                { keys: ['7e'], count: 8 },
                { keys: ['7f'], count: 4 },
                { keys: ['70'], count: 3 },
                { keys: ['71'], count: 4 },
                { keys: ['72'], count: 6 },
                { keys: ['73'], count: 4 },
                { keys: ['74'], count: 6 },
                { keys: ['75'], count: 6 },
                { keys: ['76'], count: 2 },
                { keys: ['77'], count: 6 },
                { keys: ['78'], count: 3 },
                { keys: ['79'], count: 5 },
                { keys: ['8a'], count: 2 },
                { keys: ['8b'], count: 1 },
                { keys: ['8c'], count: 4 },
                { keys: ['8d'], count: 2 },
                { keys: ['8e'], count: 4 },
                { keys: ['8f'], count: 4 },
                { keys: ['80'], count: 4 },
                { keys: ['81'], count: 3 },
                { keys: ['82'], count: 3 },
                { keys: ['83'], count: 7 },
                { keys: ['84'], count: 6 },
                { keys: ['85'], count: 4 },
                { keys: ['86'], count: 9 },
                { keys: ['87'], count: 7 },
                { keys: ['88'], count: 6 },
                { keys: ['89'], count: 9 },
                { keys: ['9a'], count: 5 },
                { keys: ['9b'], count: 6 },
                { keys: ['9c'], count: 4 },
                { keys: ['9d'], count: 3 },
                { keys: ['9e'], count: 1 },
                { keys: ['9f'], count: 4 },
                { keys: ['90'], count: 8 },
                { keys: ['91'], count: 3 },
                { keys: ['92'], count: 2 },
                { keys: ['93'], count: 10 },
                { keys: ['94'], count: 5 },
                { keys: ['95'], count: 1 },
                { keys: ['96'], count: 4 },
                { keys: ['97'], count: 5 },
                { keys: ['98'], count: 2 },
                { keys: ['99'], count: 1 },
            ];

            const slices = await gatherSlices(slicer);

            // get rid of the null
            slices.pop();

            const sliceCount = slices.reduce((prev, curr) => {
                return curr.count + prev;
            }, 0);

            expect(sliceCount).toEqual(evenBulkData.length);
            expect(slices).toEqual(expectedSlices);

            const records = await pMap(slices, async (slice) => {
                const data = await api.fetch(slice) as DataEntity[];

                return { slice, data, count: data.length };
            });

            const recordCount = records.reduce((prev, curr) => {
                return curr.count + prev;
            }, 0);

            expect(recordCount).toEqual(evenBulkData.length);
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

        it('can properly make id slicer ranges', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig,
                id_field_name: idFieldName,
                starting_key_depth: 0,
                key_type: IDType.base64url,
            };

            const api = createElasticsearchReaderAPI({
                config, client: readerClient, logger, emitter
            });

            const results = await api.makeIDSlicerRanges({
                numOfSlicers: 7,
            });

            expect(results).toEqual([
                { keys: ['A', 'H', 'O', 'V', 'c', 'j', 'q', 'x', '4', '_'], count: 132 },
                { keys: ['B', 'I', 'P', 'W', 'd', 'k', 'r', 'y', '5'], count: 113 },
                { keys: ['C', 'J', 'Q', 'X', 'e', 'l', 's', 'z', '6'], count: 111 },
                { keys: ['D', 'K', 'R', 'Y', 'f', 'm', 't', '0', '7'], count: 201 },
                { keys: ['E', 'L', 'S', 'Z', 'g', 'n', 'u', '1', '8'], count: 130 },
                { keys: ['F', 'M', 'T', 'a', 'h', 'o', 'v', '2', '9'], count: 177 },
                { keys: ['G', 'N', 'U', 'b', 'i', 'p', 'w', '3', '-'], count: 136 }
            ]);
        });

        it('can make date ranges for slicers', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const ranges = await api.makeDateSlicerRanges({
                lifecycle: 'once',
                numOfSlicers: 7,
                recoveryData: [],
            });

            if (!ranges || ranges.length === 0) {
                throw new Error('Faulty test');
            }

            const data = ranges
                .map((node) => {
                    if (node == null) return node;
                    const {
                        count,
                        dates: mDates,
                        interval,
                        range: mRange
                    } = node as DateSlicerRange;
                    return {
                        count,
                        interval,
                        dates: {
                            start: mDates.start.toISOString(),
                            end: mDates.end.toISOString(),
                            limit: mDates.limit.toISOString(),
                            ...(mDates.holes && { holes: mDates.holes })
                        },
                        range: {
                            start: mRange.start.toISOString(),
                            limit: mRange.limit.toISOString()
                        }
                    };
                });

            expect(data.length).toBeGreaterThan(0);

            expect(data).toMatchObject([
                {
                    count: 71,
                    interval: [27, 'ms'],
                    dates: {
                        start: '2019-04-26T15:00:23.201Z',
                        end: '2019-04-26T15:00:23.228Z',
                        limit: '2019-04-26T15:00:23.228Z'
                    },
                    range: {
                        start: '2019-04-26T15:00:23.201Z',
                        limit: '2019-04-26T15:00:23.394Z'
                    }
                },
                {
                    count: 89,
                    interval: [27, 'ms'],
                    dates: {
                        start: '2019-04-26T15:00:23.228Z',
                        end: '2019-04-26T15:00:23.255Z',
                        limit: '2019-04-26T15:00:23.255Z'
                    },
                    range: {
                        start: '2019-04-26T15:00:23.201Z',
                        limit: '2019-04-26T15:00:23.394Z'
                    }
                },
                {
                    count: 124,
                    interval: [27, 'ms'],
                    dates: {
                        start: '2019-04-26T15:00:23.255Z',
                        end: '2019-04-26T15:00:23.282Z',
                        limit: '2019-04-26T15:00:23.282Z'
                    },
                    range: {
                        start: '2019-04-26T15:00:23.201Z',
                        limit: '2019-04-26T15:00:23.394Z'
                    }
                },
                {
                    count: 109,
                    interval: [27, 'ms'],
                    dates: {
                        start: '2019-04-26T15:00:23.282Z',
                        end: '2019-04-26T15:00:23.309Z',
                        limit: '2019-04-26T15:00:23.309Z'
                    },
                    range: {
                        start: '2019-04-26T15:00:23.201Z',
                        limit: '2019-04-26T15:00:23.394Z'
                    }
                },
                {
                    count: 204,
                    interval: [27, 'ms'],
                    dates: {
                        start: '2019-04-26T15:00:23.309Z',
                        end: '2019-04-26T15:00:23.336Z',
                        limit: '2019-04-26T15:00:23.336Z'
                    },
                    range: {
                        start: '2019-04-26T15:00:23.201Z',
                        limit: '2019-04-26T15:00:23.394Z'
                    }
                },
                {
                    count: 118,
                    interval: [27, 'ms'],
                    dates: {
                        start: '2019-04-26T15:00:23.336Z',
                        end: '2019-04-26T15:00:23.363Z',
                        limit: '2019-04-26T15:00:23.363Z'
                    },
                    range: {
                        start: '2019-04-26T15:00:23.201Z',
                        limit: '2019-04-26T15:00:23.394Z'
                    }
                },
                {
                    count: 285,
                    interval: [31, 'ms'],
                    dates: {
                        start: '2019-04-26T15:00:23.363Z',
                        end: '2019-04-26T15:00:23.394Z',
                        limit: '2019-04-26T15:00:23.394Z'
                    },
                    range: {
                        start: '2019-04-26T15:00:23.201Z',
                        limit: '2019-04-26T15:00:23.394Z'
                    }
                }
            ]);
        });
    });
});
