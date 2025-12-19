import 'jest-extended';
import { TestClientConfig } from '@terascope/job-components';
import { debugLogger, DataEntity } from '@terascope/core-utils';
import { WorkerTestHarness, newTestJobConfig, JobTestHarness } from 'teraslice-test-harness';
import { ElasticsearchTestHelpers } from '@terascope/opensearch-client';
import {
    TEST_INDEX_PREFIX, makeClient, cleanupIndex,
    populateIndex, addToIndex
} from '../helpers/index.js';
import evenSpreadExtra1 from '../fixtures/data/even-spread-extra1.js';

describe('elasticsearch_reader fetcher', () => {
    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_fetcher_`;
    const logger = debugLogger('test-logger');

    function makeIndex(str: string) {
        return `${readerIndex}_${str}`;
    }
    const evenSpread = ElasticsearchTestHelpers.EvenDateData;
    const evenIndex = makeIndex('even_index');

    const evenBulkData = evenSpread.data;

    let workerHarness: WorkerTestHarness;
    let jobHarness: JobTestHarness;
    let esClient: any;
    let clients: TestClientConfig[];

    beforeAll(async () => {
        esClient = await makeClient();

        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: esClient,
                    logger
                }),
            },

        ];

        await cleanupIndex(esClient, makeIndex('*'));
        await populateIndex(esClient, evenIndex, evenSpread.EvenDataType, evenBulkData);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, makeIndex('*'));
    });

    afterEach(async () => {
        if (workerHarness) {
            if (workerHarness.events) workerHarness.events.emit('worker:shutdown');
            await workerHarness.shutdown();
        }

        if (jobHarness) await jobHarness.shutdown();
    });

    const defaults = {
        _name: 'elasticsearch_reader_api',
        date_field_name: 'created',
        index: evenIndex,
        time_resolution: 'ms',
    };

    async function makeFetcherTest(config: Record<string, any> = {}) {
        const apiConfig = Object.assign({}, defaults, config);

        workerHarness = WorkerTestHarness.testFetcher(
            { _op: 'elasticsearch_reader', _api_name: 'elasticsearch_reader_api' },
            apiConfig,
            { clients }
        );

        await workerHarness.initialize();

        return workerHarness;
    }

    async function makeJobTest(config: Record<string, any> = {}) {
        const apiConfig = Object.assign({}, defaults, config);
        const job = newTestJobConfig({
            apis: [apiConfig],
            operations: [
                { _op: 'elasticsearch_reader', _api_name: 'elasticsearch_reader_api' },
                { _op: 'noop' }
            ]
        });

        jobHarness = new JobTestHarness(job, { clients });

        await jobHarness.initialize();

        return jobHarness;
    }

    it('fetcher can instantiate', async () => {
        const test = await makeFetcherTest();
        expect(test).toBeDefined();
    });

    it('fetcher can return formatted data', async () => {
        // this range has 48 records
        const sliceSize = 48;
        const slice = {
            start: '2019-04-26T15:00:23.201Z',
            end: '2019-04-26T15:00:23.220Z',
            limit: '2019-04-26T15:00:23.394Z',
            count: sliceSize
        };

        const test = await makeFetcherTest({ size: 100 });
        const results = await test.runSlice(slice);

        expect(Array.isArray(results)).toEqual(true);
        expect(results).toBeArrayOfSize(sliceSize);
        const doc = results[0];
        expect(DataEntity.isDataEntity(doc)).toEqual(true);

        const metaData = doc.getMetadata();

        expect(typeof metaData._createTime).toEqual('number');
        expect(typeof metaData._processTime).toEqual('number');
        expect(typeof metaData._ingestTime).toEqual('number');
        expect(typeof metaData._eventTime).toEqual('number');

        expect(doc.getKey()).toBeString();
        expect(metaData._index).toEqual(evenIndex);

        expect(metaData._type).toBeUndefined();
    });

    describe('when more records are added to the slice range after slice creation', () => {
        const evenIndexName1 = `${TEST_INDEX_PREFIX}_elasticsearch_fetcher1_`;
        const evenSpreadExtra1BulkData = evenSpreadExtra1.data.map(
            (obj) => DataEntity.make(obj, { _key: obj.uuid })
        );

        beforeAll(async () => {
            await cleanupIndex(esClient, evenIndexName1);
            await populateIndex(
                esClient, evenIndexName1, evenSpread.EvenDataType, evenBulkData
            );
            await addToIndex(esClient, evenIndexName1, evenSpreadExtra1BulkData);
        });

        afterAll(async () => {
            await cleanupIndex(esClient, evenIndexName1);
        });

        it('the fetcher successfully retrieves all 8 records', async () => {
            // this range has 4 records to begin with (from the outer beforeAll)
            // the inner beforeAll adds 4 more, making this count "stale"
            // so the result set should contain 8 records
            const slice = {
                start: '2019-04-26T15:00:23.201Z',
                end: '2019-04-26T15:00:23.207Z',
                count: 4
            };

            const test = await makeFetcherTest({ index: evenIndexName1, size: 100 });
            const result = await test.runSlice(slice);
            expect(result.length).toEqual(8);
        });
    });

    describe('when too many records are added to the slice range after slice creation', () => {
        const evenIndexName2 = `${TEST_INDEX_PREFIX}_elasticsearch_fetcher2_`;
        const genExtraBulkData = () => evenSpreadExtra1.data.map(
            (obj) => {
                // we need random _keys to get new records rather than overwrite
                const randomSuffix = [...Array(5)].map(() => Math.floor(Math.random() * 16).toString(16)).join('');
                const newKey = obj.uuid.slice(0, -5) + randomSuffix;
                return DataEntity.make(obj, { _key: newKey });
            }
        );

        beforeAll(async () => {
            await cleanupIndex(esClient, evenIndexName2);
            await populateIndex(
                esClient, evenIndexName2, evenSpread.EvenDataType, evenBulkData
            );
            // add a bunch more records to make sure to trigger the retry failure
            await addToIndex(esClient, evenIndexName2, genExtraBulkData());
            await addToIndex(esClient, evenIndexName2, genExtraBulkData());
            await addToIndex(esClient, evenIndexName2, genExtraBulkData());
            await addToIndex(esClient, evenIndexName2, genExtraBulkData());
            await addToIndex(esClient, evenIndexName2, genExtraBulkData());
            await addToIndex(esClient, evenIndexName2, genExtraBulkData());
            await addToIndex(esClient, evenIndexName2, genExtraBulkData());
        });

        afterAll(async () => {
            await cleanupIndex(esClient, evenIndexName2);
        });

        it('the fetcher raises an error after five retries', async () => {
            // this range has 4 records to begin with (from the outer beforeAll)
            // the inner beforeAll adds 4 more, making this count "stale"
            // so the result set should contain 8 records
            const slice = {
                start: '2019-04-26T15:00:23.201Z',
                end: '2019-04-26T15:00:23.207Z',
                count: 4
            };

            const test = await makeFetcherTest({ index: evenIndexName2, size: 100 });
            // Ideally we'd be testing for the following error message, but
            // there is a bug in pRetry. See _fetch in the
            // `ElasticsearchReaderAPI` for details
            // const errMsg = 'Retry limit (5) hit, caused by Error: The result
            // set contained exactly 32 records, searching again with size: 48';
            const errMsg = 'The result set contained exactly 32 records, searching again with size: 48';
            try {
                await test.runSlice(slice);
                throw new Error('should have error');
            } catch (error) {
                expect(
                    // @ts-expect-error
                    error.message
                ).toEqual(errMsg);
            }
        });
    });

    it('can fetch the entire index', async () => {
        const test = await makeJobTest({ size: 100 });
        let recordCount = 0;

        const results = await test.runToCompletion();

        for (const sliceResult of results) {
            recordCount += sliceResult.data.length;
        }

        expect(recordCount).toEqual(evenSpread.data.length);
    });

    it('fetcher throws if query size exceeds the index.max_result_window setting', async () => {
        // this range has 48 records
        const slice = {
            start: '2019-04-26T15:00:23.201Z',
            end: '2019-04-26T15:00:23.220Z',
            limit: '2019-04-26T15:00:23.394Z',
            count: 10000
        };

        const errMsg = 'The query size, 15000, is greater than the index.max_result_window: 10000';
        try {
            const test = await makeFetcherTest({ size: 100 });
            await test.runSlice(slice);
            throw new Error('should have error');
        } catch (error) {
            expect(
                // @ts-expect-error
                error.message
            ).toEqual(errMsg);
        }
    });

    it('should throw if size is greater than window_size', async () => {
        const size = 1000000000;
        const errMsg = `Invalid parameter size: ${size}, it cannot exceed the "index.max_result_window" index setting of 10000 for index ${evenIndex}`;

        try {
            const test = await makeJobTest({ size });
            await test.runToCompletion();
            throw new Error('should have error');
        } catch (err) {
            expect(
                // @ts-expect-error
                err.message
            ).toEqual(errMsg);
        }
    });
});
