import 'jest-extended';
import { debugLogger, DataEntity } from '@terascope/utils';
import { DataFrame } from '@terascope/data-mate';
import { ElasticsearchTestHelpers } from '@terascope/opensearch-client';
import { EventEmitter } from 'node:events';
import {
    TEST_INDEX_PREFIX,
    cleanupIndex,
    populateIndex,
    waitForData,
    makeClient
} from '../helpers/index.js';
import {
    createElasticsearchReaderAPI,
    ElasticsearchReaderClient,
    ESReaderOptions,
    FetchResponseType,
    IDType,
    ReaderSlice
} from '../../src/index.js';

describe('id_reader tests', () => {
    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_api_dataframe_id_reader`;
    const logger = debugLogger('api-dataFrame-test');
    const emitter = new EventEmitter();
    const idFieldName = 'uuid';

    const evenSpread = ElasticsearchTestHelpers.EvenDateData;

    function makeIndex(str: string): string {
        return `${readerIndex}_${str}`;
    }

    const testIndex = makeIndex('id_refactored_tests');
    const evenBulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    const docType = '_doc';
    let client: any;
    let readerClient: ElasticsearchReaderClient;

    beforeAll(async () => {
        client = await makeClient();

        readerClient = new ElasticsearchReaderClient(
            client,
            { index: testIndex },
            logger,
        );
        await cleanupIndex(client, makeIndex('*'));
        await populateIndex(
            client, testIndex, evenSpread.EvenDataType, evenBulkData, docType
        );
        await waitForData(client, testIndex, evenBulkData.length);
    });

    afterAll(async () => {
        await cleanupIndex(client, makeIndex('*'));
    });

    describe('Cross cluster searching', () => {
        const defaultConfig: ESReaderOptions = Object.seal({
            index: testIndex,
            size: 1000,
            date_field_name: 'created',
            query: '*',
            type: docType,
            response_type: FetchResponseType.data_frame,
            type_config: evenSpread.EvenDataType,
            start: null,
            end: null,
            interval: 'auto',
            subslice_by_key: false,
            fields: [],
            delay: '1000s',
            id_field_name: idFieldName,
            subslice_key_threshold: 1000000,
            key_type: IDType.base64url,
            time_resolution: 'ms',
            connection: 'default',
            starting_key_depth: 0
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
                keys: ['a'], count: 58
            });
        });

        it('can fetch records', async () => {
            const api = createElasticsearchReaderAPI({
                config: defaultConfig, client: readerClient, logger, emitter
            });

            const results = await api.fetch() as DataFrame;

            expect(results).toBeInstanceOf(DataFrame);
            expect(results.size).toEqual(1000);
        });
    });
});
