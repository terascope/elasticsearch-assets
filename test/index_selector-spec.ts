import 'jest-extended';
import { DataEntity } from '@terascope/utils';
import path from 'path';
import { WorkerTestHarness } from 'teraslice-test-harness';
import { MUTATE_META, INDEX_META } from '../asset/src/elasticsearch_index_selector/interfaces';

describe('elasticsearch index selector', () => {
    const assetDir = path.join(__dirname, '..');
    let harness: WorkerTestHarness;
    let clients: any;

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    async function makeTest(_config: any) {
        const config = Object.assign({}, { _op: 'elasticsearch_index_selector', index: 'some-index', type: 'some-type' }, _config);
        if (!_config.type) delete config.type;

        harness = WorkerTestHarness.testProcessor(config, { assetDir, clients });
        await harness.initialize();
        return harness;
    }

    it('schema has defaults', async () => {
        const opName = 'elasticsearch_index_selector';
        const testOpConfig = { _op: opName, index: 'some-index', type: 'some-type' };
        const test = await makeTest(testOpConfig);
        const {
            opConfig: {
                preserve_id: preserveId,
                delete: deleteConfig,
                update,
                update_retry_on_conflict: retry
            }
        } = test.getOperation(opName);

        expect(retry).toEqual(0);
        expect(preserveId).toEqual(false);
        expect(deleteConfig).toEqual(false);
        expect(update).toEqual(false);
    });

    it('will throw if other config options are not present with timeseries', async () => {
        expect.assertions(3);

        const op1 = { _op: 'elasticsearch_index_selector', timeseries: 'daily' };
        const op2 = { _op: 'elasticsearch_index_selector', timeseries: 'daily', index_prefix: 'hello' };
        const op3 = {
            _op: 'elasticsearch_index_selector',
            timeseries: 'daily',
            index_prefix: 'hello',
            date_field: 'created'
        };

        const errMsg = 'Invalid elasticsearch_index_selector configuration';
        try {
            await makeTest(op1);
        } catch (err) {
            expect(err.message).toStartWith(errMsg);
        }

        try {
            await makeTest(op2);
        } catch (err) {
            expect(err.message).toStartWith(errMsg);
        }

        const test = await makeTest(op3);
        expect(test).toBeDefined();
    });

    it('will throw error on a bad date field in timeseries', async () => {
        expect.hasAssertions();

        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: '-',
            index_prefix: 'weekly-test',
            type: 'events',
            timeseries: 'weekly',
            date_field: 'name'
        };

        const data = [
            { _id: '1', date: '2019-07-02T00:00:00.001Z', name: 'bob' },
        ];

        const test = await makeTest(opConfig);

        try {
            await test.runSlice(data);
        } catch (err) {
            expect(err.message).toBe('Invalid opConfig date field: name. It either does not exists or is not a valid date on the records processed');
        }
    });

    it('should correctly create weekly index name', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: '-',
            index_prefix: 'weekly-test',
            type: 'events',
            timeseries: 'weekly',
            date_field: 'date'
        };

        // weekly index count needs to increment as weekly count from epoch time
        const data = [
            { _id: '1', date: '2019-07-02T00:00:00.001Z' },
            { _id: '2', date: '2019-07-03T23:59:59.999Z' },
            { _id: '3', date: '2019-07-04T00:14:01.032Z' },
            { _id: '4', date: '2019-07-11T00:06:35.672Z' }
        ];
        const indexNames = [
            'weekly-test-2582',
            'weekly-test-2582',
            'weekly-test-2583',
            'weekly-test-2584'
        ];

        const test = await makeTest(opConfig);
        const results = await test.runSlice(data);

        expect(results).toBeArrayOfSize(4);

        results.forEach((record, index) => {
            expect(record).toMatchObject(data[index]);
            const indexMeta = record.getMetadata(INDEX_META);
            expect(indexMeta).toBeDefined();
            expect(indexMeta.index._index).toEqual(indexNames[index]);
        });
    });

    it('can take an array of objects and returns properly formatted data for bulk requests', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            delete: false
        };
        const data = [{ some: 'data' }];
        const expectedMetadata = { index: { _index: 'some_index', _type: 'events' } };

        const test = await makeTest(opConfig);
        const results = await test.runSlice(data);

        expect(results).toBeArrayOfSize(1);

        results.forEach((record, index) => {
            expect(record).toMatchObject(data[index]);
            const indexMeta = record.getMetadata(INDEX_META);
            expect(indexMeta).toBeDefined();
            expect(indexMeta).toMatchObject(expectedMetadata);
        });
    });

    it('preserve_id will work the DataEntity', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            preserve_id: true,
            delete: false
        };
        const data = [{ some: 'data' }];
        const expectedMetadata = { index: { _index: 'some_index', _type: 'events', _id: 'specialID' } };

        function addTestMeta(obj: any) {
            return DataEntity.make(obj, { _key: 'specialID' });
        }

        const test = await makeTest(opConfig);
        const reader = test.getOperation('test-reader');
        // @ts-ignore
        const fn = reader.fetch.bind(reader);
        // NOTE: we do not have a good story around added meta data to testing data
        // @ts-ignore
        reader.fetch = async (incDocs: DataEntity[]) => fn(incDocs.map(addTestMeta));
        const results = await test.runSlice(data);

        expect(results).toBeArrayOfSize(1);

        results.forEach((record, index) => {
            expect(record).toMatchObject(data[index]);
            const indexMeta = record.getMetadata(INDEX_META);
            expect(indexMeta).toBeDefined();
            expect(indexMeta).toMatchObject(expectedMetadata);
        });
    });

    it('can set id to any field in data', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            id_field: 'name'
        };
        const data = [{ some: 'data', name: 'someName' }];
        const expectedMetadata = { index: { _index: 'some_index', _type: 'events', _id: 'someName' } };

        const test = await makeTest(opConfig);
        const results = await test.runSlice(data);

        expect(results).toBeArrayOfSize(1);

        results.forEach((record, index) => {
            expect(record).toMatchObject(data[index]);
            const indexMeta = record.getMetadata(INDEX_META);
            expect(indexMeta).toBeDefined();
            expect(indexMeta).toMatchObject(expectedMetadata);
        });
    });

    it('should set _type if type is present in opConfig', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            id_field: 'name'
        };
        const data = [{ some: 'data', name: 'someName' }];
        const expectedMetadata = { index: { _index: 'some_index', _type: 'events', _id: 'someName' } };

        const test = await makeTest(opConfig);
        const results = await test.runSlice(data);

        expect(results).toBeArrayOfSize(1);

        results.forEach((record, index) => {
            expect(record).toMatchObject(data[index]);
            const indexMeta = record.getMetadata(INDEX_META);
            expect(indexMeta).toBeDefined();
            expect(indexMeta).toMatchObject(expectedMetadata);
        });
    });

    it('should not set type if type is not present in opConfig', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            id_field: 'name'
        };
        const data = [{ some: 'data', name: 'someName' }];

        const test = await makeTest(opConfig);
        const results = await test.runSlice(data);
        const expectedMetadata = { index: { _index: 'some_index', _id: 'someName' } };

        expect(results).toBeArrayOfSize(1);

        results.forEach((record, index) => {
            expect(record).toMatchObject(data[index]);
            const indexMeta = record.getMetadata(INDEX_META);
            expect(indexMeta).toBeDefined();
            expect(indexMeta).toMatchObject(expectedMetadata);
        });
    });

    it('can send an update request instead of index', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            id_field: 'name',
            update_fields: ['name'],
            update_retry_on_conflict: 11,
            delete: false,
            update: true
        };
        const data = [{ some: 'data', name: 'someName' }];
        const expectedMetadata = {
            update: {
                _index: 'some_index',
                _type: 'events',
                _id: 'someName',
                retry_on_conflict: 11
            }
        };

        const expectedMutateMetadata = { doc: { name: 'someName' } };

        const test = await makeTest(opConfig);
        const results = await test.runSlice(data);

        expect(results).toBeArrayOfSize(1);

        results.forEach((record, index) => {
            expect(record).toMatchObject(data[index]);
            const indexMeta = record.getMetadata(INDEX_META);
            expect(indexMeta).toBeDefined();
            expect(indexMeta).toMatchObject(expectedMetadata);

            const mutateMeta = record.getMetadata(MUTATE_META);
            expect(mutateMeta).toBeDefined();
            expect(mutateMeta).toMatchObject(expectedMutateMetadata);
        });
    });

    it('can send a delete request instead of index', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            id_field: 'name',
            delete: true
        };
        const data = [{ some: 'data', name: 'someName' }];
        const expectedMetadata = { delete: { _index: 'some_index', _type: 'events', _id: 'someName' } };

        const test = await makeTest(opConfig);
        const results = await test.runSlice(data);

        expect(results).toBeArrayOfSize(1);

        results.forEach((record, index) => {
            expect(record).toMatchObject(data[index]);
            const indexMeta = record.getMetadata(INDEX_META);
            expect(indexMeta).toBeDefined();
            expect(indexMeta).toMatchObject(expectedMetadata);
        });
    });

    it('can upsert specified fields by passing in an array of keys matching the document', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            upsert: true,
            update_fields: ['name', 'job']
        };
        const data = [{ some: 'data', name: 'someName', job: 'to be awesome!' }];

        const test = await makeTest(opConfig);
        const results = await test.runSlice(data);

        const expectedMetadata = { update: { _index: 'some_index', _type: 'events' } };

        const expectedMutateMetadata = {
            upsert: { some: 'data', name: 'someName', job: 'to be awesome!' },
            doc: { name: 'someName', job: 'to be awesome!' }
        };

        expect(results).toBeArrayOfSize(1);

        results.forEach((record, index) => {
            expect(record).toMatchObject(data[index]);
            const indexMeta = record.getMetadata(INDEX_META);
            expect(indexMeta).toBeDefined();
            expect(indexMeta).toMatchObject(expectedMetadata);

            const mutateMeta = record.getMetadata(MUTATE_META);
            expect(mutateMeta).toBeDefined();
            expect(mutateMeta).toMatchObject(expectedMutateMetadata);
        });
    });

    it('script file to run as part of an update request', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            upsert: true,
            update_fields: [],
            script_file: 'someFile',
            script_params: { aKey: 'job' }
        };
        const data = [{ some: 'data', name: 'someName', job: 'to be awesome!' }];

        const expectedMetadata = { update: { _index: 'some_index', _type: 'events' } };

        const expectedMutateMetadata = {
            upsert: { some: 'data', name: 'someName', job: 'to be awesome!' },
            script: { file: 'someFile', params: { aKey: 'to be awesome!' } }
        };

        const test = await makeTest(opConfig);
        const results = await test.runSlice(data);

        expect(results).toBeArrayOfSize(1);

        results.forEach((record, index) => {
            expect(record).toMatchObject(data[index]);
            const indexMeta = record.getMetadata(INDEX_META);
            expect(indexMeta).toBeDefined();
            expect(indexMeta).toMatchObject(expectedMetadata);

            const mutateMeta = record.getMetadata(MUTATE_META);
            expect(mutateMeta).toBeDefined();
            expect(mutateMeta).toMatchObject(expectedMutateMetadata);
        });
    });

    it('script to run as part of an update request', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'hello',
            type: 'events',
            upsert: true,
            update_fields: [],
            script: 'ctx._source.count += add',
            script_params: { add: 'add' }
        };
        const data = [
            { count: 1, add: 2 }
        ];

        const expectedMetadata = { update: { _index: 'hello', _type: 'events' } };

        const expectedMutateMetadata = {
            upsert: { count: 1, add: 2 },
            script: {
                source: 'ctx._source.count += add',
                params: {
                    add: 2
                }
            }
        };

        const test = await makeTest(opConfig);
        const results = await test.runSlice(data);

        expect(results).toBeArrayOfSize(1);

        results.forEach((record, index) => {
            expect(record).toMatchObject(data[index]);
            const indexMeta = record.getMetadata(INDEX_META);
            expect(indexMeta).toBeDefined();
            expect(indexMeta).toMatchObject(expectedMetadata);

            const mutateMeta = record.getMetadata(MUTATE_META);
            expect(mutateMeta).toBeDefined();
            expect(mutateMeta).toMatchObject(expectedMutateMetadata);
        });
    });
});
