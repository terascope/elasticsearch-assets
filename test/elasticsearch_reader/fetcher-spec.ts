import 'jest-extended';
import { DataEntity, AnyObject } from '@terascope/job-components';
import { getESVersion } from 'elasticsearch-store';
import { WorkerTestHarness, newTestJobConfig, JobTestHarness } from 'teraslice-test-harness';
import {
    TEST_INDEX_PREFIX,
    ELASTICSEARCH_VERSION,
    makeClient,
    cleanupIndex,
    populateIndex
} from '../helpers';
import evenSpread from '../fixtures/data/even-spread';

describe('elasticsearch_reader fetcher', () => {
    const esClient = makeClient();
    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_fetcher_`;

    function makeIndex(str: string) {
        return `${readerIndex}_${str}`;
    }

    const evenIndex = makeIndex(evenSpread.index);

    const version = getESVersion(esClient);
    const docType = version === 5 ? 'events' : '_doc';

    const evenBulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    const clients = [
        {
            type: 'elasticsearch',
            endpoint: 'default',
            create: () => ({
                client: esClient
            }),
            config: {
                apiVersion: ELASTICSEARCH_VERSION
            }
        },

    ];

    let workerHarness: WorkerTestHarness;
    let jobHarness: JobTestHarness;

    beforeAll(async () => {
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
        const slice = {
            start: '2019-04-26T15:00:23.201Z',
            end: '2019-04-26T15:00:23.220Z',
            limit: '2019-04-26T15:00:23.394Z',
            count: 10000
        };

        const test = await makeFetcherTest({ size: 100 });
        const results = await test.runSlice(slice);

        expect(Array.isArray(results)).toEqual(true);
        expect(results).toBeArrayOfSize(48);
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

    it('can fetch the entire index', async () => {
        const test = await makeJobTest({ size: 100 });
        let recordCount = 0;

        const results = await test.runToCompletion();

        for (const sliceResult of results) {
            recordCount += sliceResult.data.length;
        }

        expect(recordCount).toEqual(evenSpread.data.length);
    });

    it('should throw if size is greater than window_size', async () => {
        const size = 1000000000;
        const errMsg = `Invalid parameter size: ${size}, it cannot exceed the "index.max_result_window" index setting of 10000 for index ${evenIndex}`;

        try {
            const test = await makeJobTest({ size });
            await test.runToCompletion();
        } catch (err) {
            expect(err.message).toEqual(errMsg);
        }
    });
});
