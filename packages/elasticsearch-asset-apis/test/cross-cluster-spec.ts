import 'jest-extended';
import { createClient } from 'elasticsearch-store';
import { LATEST_VERSION, DataTypeConfig } from '@terascope/data-types';
import { debugLogger, DataEntity } from '@terascope/utils';
import { DataFrame } from '@terascope/data-mate';
import { EventEmitter } from 'events';
import {
    TEST_INDEX_PREFIX,
    ELASTICSEARCH_HOST,
    cleanupIndex,
    populateIndex,
    waitForData,
} from './helpers';
import evenSpread from './fixtures/data/even-spread';
import {
    createElasticsearchReaderAPI,
    ElasticsearchReaderClient,
    ESReaderOptions,
    FetchResponseType,
    IDType,
    ReaderSlice
} from '../src';

describe('Reader API', () => {
    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_api_dataframe_cross_cluster`;
    const logger = debugLogger('api-dataFrame-test');
    const emitter = new EventEmitter();
    const idFieldName = 'uuid';
    const baseIndex = 'es_d1-foo-v1-bar';
    const queryIndex = 'es_d*-foo-v1-*';

    function makeIndex(str: string): string {
        return `${readerIndex}_${str}`;
    }

    const crossClusterTestIndex = makeIndex(baseIndex);
    const evenBulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    const docType = '_doc';
    let client: any;
    let readerClient: ElasticsearchReaderClient;

    beforeAll(async () => {
        const { client: esClient, } = await createClient({
            node: ELASTICSEARCH_HOST,
        } as any, logger);
        client = esClient;

        readerClient = new ElasticsearchReaderClient(
            client,
            { index: crossClusterTestIndex },
            logger,
        );
        await cleanupIndex(client, makeIndex('*'));
        await populateIndex(client, crossClusterTestIndex, evenSpread.types, evenBulkData, docType);
        await waitForData(client, crossClusterTestIndex, evenBulkData.length);
    });

    afterAll(async () => {
        await cleanupIndex(client, makeIndex('*'));
    });

    const typeConfig: DataTypeConfig = {
        version: LATEST_VERSION,
        fields: evenSpread.types
    };

    describe('Cross cluster searching', () => {
        const defaultConfig: ESReaderOptions = Object.seal({
            index: makeIndex(queryIndex),
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
