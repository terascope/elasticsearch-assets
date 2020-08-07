import { WorkerTestHarness, SlicerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import {
    AnyObject, DataEntity, SlicerRecoveryData, TestContext
} from '@terascope/job-components';
import path from 'path';
import MockClient from '../mock_client';
import Schema from '../../asset/src/id_reader/schema';

describe('id_reader', () => {
    const assetDir = path.join(__dirname, '..');
    let harness: WorkerTestHarness | SlicerTestHarness;
    let clients: any;
    let defaultClient: MockClient;

    beforeEach(() => {
        defaultClient = new MockClient();
        clients = [
            {
                type: 'elasticsearch',
                endpoint: 'default',
                create: () => ({
                    client: defaultClient
                }),
            },
            {
                type: 'elasticsearch',
                endpoint: 'otherConnection',
                create: () => ({
                    client: new MockClient()
                }),
            }
        ];
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    async function makeFetcherTest(config: any) {
        harness = WorkerTestHarness.testFetcher(config, { assetDir, clients });
        await harness.initialize();
        return harness;
    }

    async function makeSlicerTest(
        config: any,
        numOfSlicers = 1,
        recoveryData?: SlicerRecoveryData[]
    ) {
        const job = newTestJobConfig({
            analytics: true,
            slicers: numOfSlicers,
            operations: [
                config,
                {
                    _op: 'noop'
                }
            ]
        });
        harness = new SlicerTestHarness(job, { assetDir, clients });
        await harness.initialize(recoveryData);
        return harness;
    }

    describe('schema', () => {
        it('can validateJob to make sure its configured correctly', () => {
            const errorStr1 = 'The number of slicers specified on the job cannot be more the length of key_range';
            const errorStr2 = 'The number of slicers specified on the job cannot be more than 16';
            const errorStr3 = 'The number of slicers specified on the job cannot be more than 64';

            const job1 = { slicers: 1, operations: [{ _op: 'id_reader', index: 'some-index', key_range: ['a', 'b'] }] };
            const job2 = { slicers: 2, operations: [{ _op: 'id_reader', index: 'some-index', key_range: ['a'] }] };
            const job3 = { slicers: 4, operations: [{ _op: 'id_reader', index: 'some-index', key_type: 'hexadecimal' }] };
            const job4 = { slicers: 20, operations: [{ _op: 'id_reader', index: 'some-index', key_type: 'hexadecimal' }] };
            const job5 = { slicers: 20, operations: [{ _op: 'id_reader', index: 'some-index', key_type: 'base64url' }] };
            const job6 = { slicers: 70, operations: [{ _op: 'id_reader', index: 'some-index', key_type: 'base64url' }] };

            function testValidation(job: AnyObject) {
                const context = new TestContext('test');
                const schema = new Schema(context);
                schema.validateJob(job as any);
            }

            expect(() => {
                testValidation(job1);
            }).not.toThrow();
            expect(() => {
                testValidation(job2);
            }).toThrowError(errorStr1);

            expect(() => {
                testValidation(job3);
            }).not.toThrow();
            expect(() => {
                testValidation(job4);
            }).toThrowError(errorStr2);

            expect(() => {
                testValidation(job5);
            }).not.toThrow();
            expect(() => {
                testValidation(job6);
            }).toThrowError(errorStr3);

            expect(() => {
                testValidation(job5);
            }).not.toThrow();
            expect(() => {
                testValidation(job6);
            }).toThrowError(errorStr3);
        });
    });

    describe('slicer', () => {
        it('can create a slicer', async () => {
            const opConfig = {
                _op: 'id_reader',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                field: 'someField',
                index: 'someindex'
            };

            const test = await makeSlicerTest(opConfig);
            const slicer = test.slicer();
            expect(slicer.slicers()).toEqual(1);
        });

        it('can create multiple slicers', async () => {
            const opConfig = {
                _op: 'id_reader',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                field: 'someField',
                index: 'someindex'
            };

            const test = await makeSlicerTest(opConfig, 2);
            const slicer = test.slicer();

            expect(slicer.slicers()).toEqual(2);
        });

        it('a single slicer can produces values', async () => {
            const opConfig = {
                _op: 'id_reader',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                field: 'someField',
                index: 'someindex',
                size: 200
            };

            const test = await makeSlicerTest(opConfig);

            const [slice1] = await test.createSlices();
            expect(slice1).toEqual({ count: 100, wildcard: { field: 'someField', value: 'a*' } });

            const [slice2] = await test.createSlices();
            expect(slice2).toEqual({ count: 100, wildcard: { field: 'someField', value: 'b*' } });

            const slice3 = await test.createSlices();
            expect(slice3).toEqual([null]);
        });

        it('produces values starting at a specific depth', async () => {
            const opConfig = {
                _op: 'id_reader',
                key_type: 'hexadecimal',
                key_range: ['a', 'b', 'c', 'd'],
                starting_key_depth: 3,
                field: 'someField',
                index: 'someindex',
                size: 200
            };

            const test = await makeSlicerTest(opConfig);

            const [slice1] = await test.createSlices();
            expect(slice1).toEqual({ count: 100, wildcard: { field: 'someField', value: 'a00*' } });

            const [slice2] = await test.createSlices();
            expect(slice2).toEqual({ count: 100, wildcard: { field: 'someField', value: 'a01*' } });

            const [slice3] = await test.createSlices();
            expect(slice3).toEqual({ count: 100, wildcard: { field: 'someField', value: 'a02*' } });
        });

        it('produces values even with an initial search error', async () => {
            const opConfig = {
                _op: 'id_reader',
                field: 'someField',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                index: 'someindex',
                size: 200
            };
            const { sequence } = defaultClient;
            sequence.pop();
            defaultClient.sequence = [{
                _shards: {
                    failed: 1,
                    failures: [{ reason: { type: 'some Error' } }]
                }
            },
            ...sequence
            ];

            const test = await makeSlicerTest(opConfig);

            const [slice1] = await test.createSlices();
            expect(slice1).toEqual({ count: 100, wildcard: { field: 'someField', value: 'a*' } });

            const [slice2] = await test.createSlices();
            expect(slice2).toEqual({ count: 100, wildcard: { field: 'someField', value: 'b*' } });

            const slice3 = await test.createSlices();
            expect(slice3).toEqual([null]);
        });

        it('key range gets divided up by number of slicers', async () => {
            const opConfig = {
                _op: 'id_reader',
                field: 'someField',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                index: 'someindex',
                size: 200
            };

            const test = await makeSlicerTest(opConfig, 2);

            const slices1 = await test.createSlices();

            expect(slices1[0]).toEqual({ count: 100, wildcard: { field: 'someField', value: 'a*' } });
            expect(slices1[1]).toEqual({ count: 100, wildcard: { field: 'someField', value: 'b*' } });

            const slices2 = await test.createSlices();

            expect(slices2).toEqual([null, null]);
        });

        it('key range gets divided up by number of slicers by size', async () => {
            const newSequence = [
                { _shards: { failed: 0 }, hits: { total: 100 } },
                { _shards: { failed: 0 }, hits: { total: 500 } },
                { _shards: { failed: 0 }, hits: { total: 200 } },
                { _shards: { failed: 0 }, hits: { total: 200 } },
                { _shards: { failed: 0 }, hits: { total: 100 } }
            ];

            const opConfig = {
                _op: 'id_reader',
                field: 'someField',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                index: 'someindex',
                size: 200
            };
            defaultClient.sequence = newSequence;

            const test = await makeSlicerTest(opConfig);

            const [slice1] = await test.createSlices();
            expect(slice1).toEqual({ count: 100, wildcard: { field: 'someField', value: 'a*' } });

            const [slice2] = await test.createSlices();
            expect(slice2).toEqual({ count: 200, wildcard: { field: 'someField', value: 'b0*' } });

            const [slice3] = await test.createSlices();
            expect(slice3).toEqual({ count: 200, wildcard: { field: 'someField', value: 'b1*' } });

            const [slice4] = await test.createSlices();
            expect(slice4).toEqual({ count: 100, wildcard: { field: 'someField', value: 'b2*' } });

            const slice5 = await test.createSlices();
            expect(slice5).toEqual([null]);
        });

        it('can return to previous position', async () => {
            const retryData = [{ lastSlice: { key: 'events-#a6*' }, slicer_id: 0 }];
            const opConfig = {
                _op: 'id_reader',
                field: 'someField',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                index: 'someindex',
                size: 200
            };

            const test = await makeSlicerTest(opConfig, 1, retryData);

            const [slice1] = await test.createSlices();
            expect(slice1).toEqual({ count: 100, wildcard: { field: 'someField', value: 'a7*' } });

            const [slice2] = await test.createSlices();
            expect(slice2).toEqual({ count: 100, wildcard: { field: 'someField', value: 'a8*' } });

            const [slice3] = await test.createSlices();
            expect(slice3).toEqual({ count: 100, wildcard: { field: 'someField', value: 'a9*' } });

            const [slice4] = await test.createSlices();
            expect(slice4).toEqual({ count: 100, wildcard: { field: 'someField', value: 'aa*' } });

            const [slice5] = await test.createSlices();
            expect(slice5).toEqual({ count: 100, wildcard: { field: 'someField', value: 'ab*' } });
        });
    });

    describe('fetcher', () => {
        it('can search and fetch data from elasticsearch', async () => {
            const opConfig = {
                _op: 'id_reader',
                field: 'someField',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                index: 'someindex',
                size: 200
            };
            const slice = { count: 100, wildcard: { field: 'someField', value: 'a*' } };
            const finalQuery = {
                index: 'someindex',
                size: 100,
                body: {
                    query: {
                        bool: {
                            must: [
                                {
                                    wildcard: { [opConfig.field]: 'a*' }
                                }
                            ]
                        }
                    }
                }
            };

            const test = await makeFetcherTest(opConfig);
            const [results] = await test.runSlice(slice);

            expect(defaultClient.searchQuery).toEqual(finalQuery);
            expect(results).toBeDefined();
            expect(DataEntity.isDataEntity(results)).toEqual(true);

            const metaData = results.getMetadata();

            expect(typeof metaData._createTime).toEqual('number');
            expect(typeof metaData._processTime).toEqual('number');
            expect(typeof metaData._ingestTime).toEqual('number');
            expect(typeof metaData._eventTime).toEqual('number');

            expect(results.getKey()).toEqual('someId');
            expect(metaData._index).toEqual('test-index');
            expect(metaData._type).toEqual('test-type');
            expect(metaData._version).toEqual(1);
        });
    });
});
