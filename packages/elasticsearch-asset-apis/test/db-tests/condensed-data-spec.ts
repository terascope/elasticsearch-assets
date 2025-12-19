import 'jest-extended';
import { EventEmitter } from 'node:events';
import { debugLogger, DataEntity } from '@terascope/core-utils';
import {
    TEST_INDEX_PREFIX,
    cleanupIndex,
    populateIndex,
    waitForData,
    makeClient
} from '../helpers';
import condensedData from '../fixtures/data/condensed-spread.js';
import {
    createElasticsearchReaderAPI,
    ESReaderOptions,
    IDType,
    InputDateSegments,
    DateSlicerArgs,
    ElasticsearchReaderClient,
    ReaderSlice,
    FetchResponseType
} from '../../src/index.js';

describe('ReaderAPI with condensed time data', () => {
    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_api_condensed_`;
    const logger = debugLogger('api-condensed-test');
    const emitter = new EventEmitter();

    const condensedIndex = makeIndex(condensedData.index);
    const evenBulkData = condensedData.data.map((obj) => DataEntity.make(obj, { _key: obj.bytes }));

    function makeIndex(str: string) {
        return `${readerIndex}_${str}`;
    }
    let client: any;
    let readerClient: ElasticsearchReaderClient;

    beforeAll(async () => {
        client = await makeClient();

        readerClient = new ElasticsearchReaderClient(
            client,
            { index: condensedIndex },
            logger,
        );

        await cleanupIndex(client, makeIndex('*'));
        await populateIndex(
            client, condensedIndex, condensedData.CondensedDataType, evenBulkData
        );
        await waitForData(client, condensedIndex, evenBulkData.length);
    });

    afterAll(async () => {
        await cleanupIndex(client, makeIndex('*'));
    });

    const defaultConfig: ESReaderOptions = Object.seal({
        index: condensedIndex,
        size: 100_000,
        date_field_name: 'created',
        query: '*',
        response_type: FetchResponseType.data_entities,
        type_config: condensedData.CondensedDataType,
        start: null,
        end: null,
        interval: 'auto',
        subslice_by_key: false,
        fields: [],
        delay: '1000s',
        subslice_key_threshold: 1000000,
        key_type: IDType.base64url,
        time_resolution: 'ms',
        _connection: 'default',
        starting_key_depth: 0
    });

    it('can determine slice interval', async () => {
        // this tests scenario when there are high number of slices but low
        const config: ESReaderOptions = {
            ...defaultConfig,
            size: 100
        };

        const api = createElasticsearchReaderAPI({
            config, client: readerClient, logger, emitter
        });

        const dates = await api.determineDateRanges() as InputDateSegments;
        const result = await api.determineSliceInterval(config.interval, dates);

        expect(result).toEqual({
            interval: [1, 'ms'],
            count: 2000
        });
    });

    it('can slice and read data this small', async () => {
        const config: ESReaderOptions = {
            ...defaultConfig,
            size: 100
        };

        const slicerConfig: DateSlicerArgs = {
            lifecycle: 'once',
            slicerID: 0,
            numOfSlicers: 1,
        };

        const api = createElasticsearchReaderAPI({
            config, client: readerClient, logger, emitter
        });

        const slicer = await api.makeDateSlicer(slicerConfig);

        const slice = await slicer();
        if (!slice) throw new Error('slice should not be undefined');
        if (Array.isArray(slice)) throw new Error('slice should not be an array of slices');

        const firstResults = await api.fetch(slice as ReaderSlice);

        expect(firstResults).toBeArrayOfSize(1_000);

        const slice2 = await slicer();
        if (!slice2) throw new Error('slice should not be undefined');
        if (Array.isArray(slice2)) throw new Error('slice should not be an array of slices');

        const secondResults = await api.fetch(slice2 as ReaderSlice);

        expect(secondResults).toBeArrayOfSize(1_000);

        const slice3 = await slicer();

        expect(slice3).toBeNull();
    });
});
