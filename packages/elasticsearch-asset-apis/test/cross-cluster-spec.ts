import 'jest-extended';
import { getESVersion } from 'elasticsearch-store';
import { LATEST_VERSION, DataTypeConfig } from '@terascope/data-types';
import { debugLogger, DataEntity } from '@terascope/utils';
import { DataFrame } from '@terascope/data-mate';
import { EventEmitter } from 'events';
import {
    TEST_INDEX_PREFIX,
    makeClient,
    cleanupIndex,
    populateIndex,
    waitForData,
    formatWildcardQuery
} from './helpers';
import evenSpread from './fixtures/data/even-spread';
import {
    createElasticsearchReaderAPI,
    ElasticsearchReaderClient,
    ESReaderOptions,
    IDType,
    SlicerDateResults
} from '../src';

describe('Reader API', () => {
    const client = makeClient();
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

    const readerClient = new ElasticsearchReaderClient(
        client,
        { index: crossClusterTestIndex },
        logger,
    );

    const evenBulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    const version = getESVersion(client);
    const docType = version === 5 ? 'events' : '_doc';

    beforeAll(async () => {
        await cleanupIndex(client, makeIndex('*'));
        await populateIndex(client, crossClusterTestIndex, evenSpread.types, evenBulkData, docType);
        await waitForData(client, crossClusterTestIndex, evenBulkData.length);
    });

    afterAll(async () => {
        // await cleanupIndex(client, makeIndex('*'));
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
            use_data_frames: true,
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
            starting_key_depth: 1
        });

        it('can make date slices', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client: readerClient, logger, emitter
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

        it('can make id slices', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client: readerClient, logger, emitter
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

        it('can fetch records', async () => {
            const config: ESReaderOptions = {
                ...defaultConfig
            };

            const api = await createElasticsearchReaderAPI({
                config, client: readerClient, logger, emitter
            });

            const results = await api.fetch() as DataFrame;

            expect(results).toBeDefined();
            expect(results.size).toEqual(1000);
        });
    });
});
