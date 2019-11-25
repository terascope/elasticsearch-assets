/* eslint-disable @typescript-eslint/no-unused-vars */

import { WorkerTestHarness, SlicerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import path from 'path';
import MockClient from './mock_client';

describe('elasticsearch_data_generator', () => {
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
        recoveryData?: object[],
        size?: number
    ) {
        const job = newTestJobConfig({
            analytics: true,
            slicers: numOfSlicers,
            operations: [
                config,
                {
                    _op: 'noop',
                    size
                }
            ]
        });
        harness = new SlicerTestHarness(job, { assetDir, clients });
        await harness.initialize(recoveryData);
        return harness;
    }

    describe('fetcher', () => {
        it('returns a function that produces generated data', async () => {
            const opConfig = { _op: 'elasticsearch_data_generator' };

            const test = await makeFetcherTest(opConfig);
            const results = await test.runSlice({ count: 1 });

            expect(results.length).toEqual(1);
            expect(Object.keys(results[0]).length).toBeGreaterThan(1);
        });

        it('is backward compatable', async () => {
            const opConfig = { _op: 'elasticsearch_data_generator' };

            const test = await makeFetcherTest(opConfig);
            // @ts-ignore
            const results = await test.runSlice(1);

            expect(results.length).toEqual(1);
            expect(Object.keys(results[0]).length).toBeGreaterThan(1);
        });
    });

    describe('slicer', () => {
        it('in "once" mode will return number based off total size of last op ', async () => {
            const opConfig = { _op: 'elasticsearch_data_generator', size: 15 };
            const test = await makeSlicerTest(opConfig, 1, [], 5);

            const [slice1] = await test.createSlices();
            expect(slice1).toEqual({ count: 5 });

            const [slice2] = await test.createSlices();
            expect(slice2).toEqual({ count: 5 });

            const [slice3] = await test.createSlices();
            expect(slice3).toEqual({ count: 5 });

            const [slice4] = await test.createSlices();
            expect(slice4).toEqual(null);
        });

        it('in "once" mode will return number based off total size, which can consume it all', async () => {
            const opConfig = { _op: 'elasticsearch_data_generator', size: 15 };
            const test = await makeSlicerTest(opConfig, 1, [], 5000);

            const [slice1] = await test.createSlices();
            expect(slice1).toEqual({ count: 15 });

            const [slice2] = await test.createSlices();
            expect(slice2).toEqual(null);

            const [slice3] = await test.createSlices();
            expect(slice3).toEqual(null);

            const [slice4] = await test.createSlices();
            expect(slice4).toEqual(null);
        });

        it('slicer in "persistent" mode will continuously produce the same number', async () => {
            const opConfig = { _op: 'elasticsearch_data_generator', size: 550 };
            const job = newTestJobConfig({
                analytics: true,
                lifecycle: 'persistent',
                slicers: 1,
                operations: [
                    opConfig,
                    {
                        _op: 'noop',
                        size: 5000
                    }
                ]
            });
            const testHarness = new SlicerTestHarness(job, { assetDir, clients });
            await testHarness.initialize();

            const results1 = await testHarness.createSlices();
            const results2 = await testHarness.createSlices();
            const results3 = await testHarness.createSlices();

            expect(results1).toEqual([{ count: 550 }]);
            expect(results2).toEqual([{ count: 550 }]);
            expect(results3).toEqual([{ count: 550 }]);

            await testHarness.shutdown();
        });

        it('data generator will only return one slicer', async () => {
            const opConfig = { _op: 'elasticsearch_data_generator', size: 550 };
            const job = newTestJobConfig({
                analytics: true,
                lifecycle: 'persistent',
                slicers: 3,
                operations: [
                    opConfig,
                    {
                        _op: 'noop',
                        size: 5000
                    }
                ]
            });
            const testHarness = new SlicerTestHarness(job, { assetDir, clients });
            await testHarness.initialize();
            const slicer = testHarness.slicer();

            expect(slicer.slicers()).toEqual(1);
            await testHarness.shutdown();
        });
    });
});
