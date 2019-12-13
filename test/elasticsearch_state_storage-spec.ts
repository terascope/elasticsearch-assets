import path from 'path';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { DataEntity, AnyObject } from '@terascope/job-components';

describe('elasticsearch state storage api', () => {
    const idField = '_key';

    class TestClient {
        getData!: AnyObject;
        mgetData!: AnyObject[]
        bulkRequest!: AnyObject;

        setGetData(data: AnyObject) {
            this.getData = data;
        }

        setMGetData(data: AnyObject[]) {
            this.mgetData = data;
        }

        async get() {
            return this.getData;
        }

        async mget() {
            return this.mgetData;
        }

        async bulk(request: AnyObject) {
            this.bulkRequest = request.body;
            return request;
        }
    }

    const client = new TestClient();

    function addTestMeta(obj: any, index: number) {
        return DataEntity.make(obj, { [idField]: index + 1 });
    }

    const docArray = [
        {
            data: 'thisIsFirstData'
        },
        {
            data: 'thisIsSecondData'
        },
        {
            data: 'thisIsThirdData'
        }
    ];

    const clientConfig = {
        type: 'elasticsearch',
        create() {
            return { client };
        }
    };

    const job = newTestJobConfig({
        max_retries: 3,
        apis: [
            {
                _name: 'elasticsearch_state_storage:foo',
                index: 'someIndex',
                cache_size: (2 ** 16) - 1,
                type: 'type'
            },
            {
                _name: 'elasticsearch_state_storage:bar',
                index: 'someIndex',
                cache_size: (2 ** 16) - 1,
                type: 'type'
            }
        ],
        operations: [
            {
                _op: 'test-reader',
                passthrough_slice: true
            },
            {
                _op: 'noop',
                state_storage_api: 'elasticsearch_state_storage:foo'
            },
            {
                _op: 'noop',
                state_storage_api: 'elasticsearch_state_storage:bar'
            }
        ],
    });

    let harness: WorkerTestHarness;
    let noopFoo: any;
    let noopBar: any;
    let countFoo: number;
    let countBar: number;

    beforeEach(async () => {
        harness = new WorkerTestHarness(job, {
            assetDir: path.join(__dirname, '../asset'),
            clients: [clientConfig],
        });

        noopFoo = harness.getOperation(1);
        noopBar = harness.getOperation(2);
        const reader = harness.getOperation('test-reader');
        // @ts-ignore
        const fn = reader.fetch.bind(reader);
        // NOTE: we do not have a good story around added meta data to testing data
        // @ts-ignore
        reader.fetch = async (incDocs: DataEntity[]) => fn(incDocs.map(addTestMeta));

        noopFoo.onBatch = async (docs: DataEntity[]) => {
            const results = [];
            const { state_storage_api: name } = noopFoo.opConfig;
            const stateStorage = noopFoo.getAPI(name);
            await stateStorage.mset(docs);
            countFoo = stateStorage.count();
            const cached = await stateStorage.mget(docs);

            // eslint-disable-next-line guard-for-in
            for (const key in cached) {
                results.push(cached[key]);
            }
            return results;
        };

        noopBar.onBatch = async (docs: DataEntity[]) => {
            const { state_storage_api: name } = noopBar.opConfig;
            const stateStorage = noopFoo.getAPI(name);
            countBar = stateStorage.count();
            return docs;
        };

        await harness.initialize();
    });

    afterEach(async () => {
        await harness.shutdown();
    });

    it('can run and use the api', async () => {
        const results = await harness.runSlice(docArray);

        expect(countFoo).toEqual(3);
        expect(countBar).toEqual(0);
        expect(results.length).toEqual(3);

        results.forEach((obj: DataEntity, ind: number) => {
            expect(obj).toEqual(docArray[ind]);
            expect(DataEntity.isDataEntity(obj)).toEqual(true);
        });
    });
});
