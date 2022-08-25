import 'jest-extended';
import { DataEntity, AnyObject } from '@terascope/job-components';
import { WorkerTestHarness, newTestJobConfig, JobTestHarness } from 'teraslice-test-harness';
import {
    TEST_INDEX_PREFIX,
    makeClient,
    cleanupIndex,
    populateIndex,
    addToIndex
} from '../helpers';
import evenSpread from '../fixtures/data/even-spread';
import evenSpreadExtra1 from '../fixtures/data/even-spread-extra1';

describe('elasticsearch_reader fetcher', () => {
    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_fetcher_`;

    function makeIndex(str: string) {
        return `${readerIndex}_${str}`;
    }

    const evenIndex = makeIndex(evenSpread.index);
    const docType = '_doc';

    const evenBulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    let workerHarness: WorkerTestHarness;
    let jobHarness: JobTestHarness;
    let esClient: any;
    let clients: any;

    beforeAll(async () => {
        esClient = await makeClient();

        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: esClient
                }),
            },

        ];

        await cleanupIndex(esClient, makeIndex('*'));
        await populateIndex(esClient, evenIndex, evenSpread.types, evenBulkData, docType);
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
        _op: 'elasticsearch_reader',
        date_field_name: 'created',
        index: evenIndex,
        time_resolution: 'ms',
        type: docType
    };

    async function makeFetcherTest(config: AnyObject = {}) {
        const opConfig = Object.assign({}, defaults, config);
        workerHarness = WorkerTestHarness.testFetcher(opConfig, { clients });

        await workerHarness.initialize();

        return workerHarness;
    }

    async function makeJobTest(config: AnyObject = {}) {
        const opConfig = Object.assign({}, defaults, config);
        const job = newTestJobConfig({
            operations: [
                opConfig,
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
        expect(metaData._type).toEqual(docType);
    });

    describe('when more records are added to the slice range after slice creation', () => {
        const evenSpreadExtra1BulkData = evenSpreadExtra1.data.map(
            (obj) => DataEntity.make(obj, { _key: obj.uuid })
        );

        beforeAll(async () => {
            await addToIndex(
                esClient, evenIndex, evenSpreadExtra1BulkData, docType
            );
        });

        // since I modify the indices in the beforeAll, I have to put them
        // back right so the other tests will pass, I'm a little concerned that
        // this could result in other test failures.  So if we start getting
        // weird data consistency related test failures elsewhere, try
        // commenting out this whole inner describe
        afterAll(async () => {
            await cleanupIndex(esClient, makeIndex('*'));
            await populateIndex(esClient, evenIndex, evenSpread.types, evenBulkData, docType);
        });

        it('the fetcher successfully retrieves all 8 records', async () => {
            // this range has 4 records to begin with (from the outer beforeAll)
            // the inner beforeAll adds 4 more, making this count "stale"
            // so the result set should contain 8 records
            const slice = {
                start: '2019-04-26T15:00:23.201Z',
                end: '2019-04-26T15:00:23.207Z',
                // limit: '2019-04-26T15:00:23.394Z',
                count: 4
            };

            const test = await makeFetcherTest({ size: 100 });
            const result = await test.runSlice(slice);
            expect(result.length).toEqual(8);
        });
    });

    describe('when too many records are added to the slice range after slice creation', () => {
        const genExtraBulkData = () => evenSpreadExtra1.data.map(
            (obj) => {
                // we need random _keys to get new records rather than overwrite
                const randomSuffix = [...Array(5)].map(() => Math.floor(Math.random() * 16).toString(16)).join('');
                const newKey = obj.uuid.slice(0, -5) + randomSuffix;
                return DataEntity.make(obj, { _key: newKey });
            }
        );

        beforeAll(async () => {
            // add a bunch more records to make sure to trigger the retry failure
            await addToIndex(esClient, evenIndex, genExtraBulkData(), docType);
            await addToIndex(esClient, evenIndex, genExtraBulkData(), docType);
            await addToIndex(esClient, evenIndex, genExtraBulkData(), docType);
            await addToIndex(esClient, evenIndex, genExtraBulkData(), docType);
            await addToIndex(esClient, evenIndex, genExtraBulkData(), docType);
            await addToIndex(esClient, evenIndex, genExtraBulkData(), docType);
            await addToIndex(esClient, evenIndex, genExtraBulkData(), docType);
        });

        // since I modify the indices in the beforeAll, I have to put them
        // back right so the other tests will pass, I'm a little concerned that
        // this could result in other test failures.  So if we start getting
        // weird data consistency related test failures elsewhere, try
        // commenting out this whole inner describe
        afterAll(async () => {
            await cleanupIndex(esClient, makeIndex('*'));
            await populateIndex(esClient, evenIndex, evenSpread.types, evenBulkData, docType);
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

            const test = await makeFetcherTest({ size: 100 });
            // Ideally we'd be testing for the following error message, but
            // there is a bug in pRetry. See _fetch in the
            // `ElasticsearchReaderAPI` for details
            // const errMsg = 'Retry limit (5) hit, caused by Error: The result set contained exactly 32 records, searching again with size: 48';
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
