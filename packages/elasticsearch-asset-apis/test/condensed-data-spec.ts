import 'jest-extended';
import { EventEmitter } from 'events';
import { LATEST_VERSION, DataTypeConfig } from '@terascope/data-types';
import { debugLogger, DataEntity } from '@terascope/utils';
import { getESVersion } from 'elasticsearch-store';
import {
    TEST_INDEX_PREFIX,
    makeClient,
    cleanupIndex,
    populateIndex,
    waitForData,
} from './helpers';
import condensedData from './fixtures/data/condensed-spread';
import {
    createElasticsearchReaderAPI,
    ESReaderOptions,
    IDType,
    InputDateSegments,
    DateSlicerArgs,
    ElasticsearchReaderClient
} from '../src';

describe('ReaderAPI with condensed time data', () => {
    const client = makeClient();

    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_api_dataframe_`;
    const logger = debugLogger('api-condensed-test');
    const emitter = new EventEmitter();

    const condensedIndex = makeIndex(condensedData.index);
    const evenBulkData = condensedData.data.map((obj) => DataEntity.make(obj, { _key: obj.bytes }));

    const readerClient = new ElasticsearchReaderClient(
        client,
        { index: condensedIndex },
        logger,
    );

    const version = getESVersion(client);
    const docType = version === 5 ? 'events' : '_doc';

    function makeIndex(str: string) {
        return `${readerIndex}_${str}`;
    }

    beforeAll(async () => {
        await cleanupIndex(client, makeIndex('*'));
        await populateIndex(client, condensedIndex, condensedData.types, evenBulkData, docType);
        await waitForData(client, condensedIndex, evenBulkData.length);
    });

    afterAll(async () => {
        await cleanupIndex(client, makeIndex('*'));
    });

    const typeConfig: DataTypeConfig = {
        version: LATEST_VERSION,
        fields: condensedData.types
    };

    const defaultConfig: ESReaderOptions = Object.seal({
        index: condensedIndex,
        size: 100_000,
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
        starting_key_depth: 0
    });

    it('can determine slice interval', async () => {
        // this tests scenario when there are high number of slices but low
        const config: ESReaderOptions = {
            ...defaultConfig,
            size: 100
        };

        const api = await createElasticsearchReaderAPI({
            config, client: readerClient, logger, emitter
        });

        const dates = await api.determineDateRanges() as InputDateSegments;
        const interval = await api.determineSliceInterval(config.interval, dates);

        expect(interval).toEqual([1, 'ms']);
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

        const api = await createElasticsearchReaderAPI({
            config, client: readerClient, logger, emitter
        });

        const slicer = await api.makeDateSlicer(slicerConfig);

        const slice = await slicer();
        if (!slice) throw new Error('slice should not be undefined');
        if (Array.isArray(slice)) throw new Error('slice should not be an array of slices');

        const firstResults = await api.fetch(slice);

        expect(firstResults).toBeArrayOfSize(1_000);

        const slice2 = await slicer();
        if (!slice2) throw new Error('slice should not be undefined');
        if (Array.isArray(slice2)) throw new Error('slice should not be an array of slices');

        const secondResults = await api.fetch(slice2);

        expect(secondResults).toBeArrayOfSize(1_000);

        const slice3 = await slicer();

        expect(slice3).toBeNull();
    });
});
