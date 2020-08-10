import { WorkerTestHarness } from 'teraslice-test-harness';
import path from 'path';

describe('elasticsearch_data_generator fetcher', () => {
    const assetDir = path.join(__dirname, '..');
    let harness: WorkerTestHarness;
    let clients: any;

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    async function makeFetcherTest(config: any) {
        harness = WorkerTestHarness.testFetcher(config, { assetDir, clients });
        await harness.initialize();
        return harness;
    }

    it('returns a function that produces generated data', async () => {
        const opConfig = { _op: 'elasticsearch_data_generator' };

        const test = await makeFetcherTest(opConfig);
        const results = await test.runSlice({ count: 1 });

        expect(results.length).toEqual(1);
        expect(Object.keys(results[0]).length).toBeGreaterThan(1);
    });
});
