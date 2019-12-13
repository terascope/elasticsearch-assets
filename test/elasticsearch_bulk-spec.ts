import { WorkerTestHarness } from 'teraslice-test-harness';
import { startsWith, cloneDeep, isPlainObject } from '@terascope/job-components';
import path from 'path';
import MockClient from './mock_client';

// TODO: current bug in convict prevents testing connection_map without a *
// TODO: test flush scenarios/retries
describe('elasticsearch_bulk', () => {
    const assetDir = path.join(__dirname, '..');
    let harness: WorkerTestHarness;
    let clients: any;

    beforeEach(() => {
        clients = [
            {
                type: 'elasticsearch',
                endpoint: 'default',
                create: () => ({
                    client: new MockClient()
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

    async function makeTest(config: any) {
        harness = WorkerTestHarness.testProcessor(config, { assetDir, clients });
        await harness.initialize();
        return harness;
    }

    it('schema has defaults', async () => {
        const opName = 'elasticsearch_bulk';
        const testOpConfig = { _op: opName };
        const test = await makeTest(testOpConfig);
        const { opConfig: { multisend, size } } = test.getOperation(opName);

        expect(size).toEqual(500);
        expect(multisend).toEqual(false);
    });

    it('if no docs, returns a promise of passed in data', async () => {
        const opConfig = { _op: 'elasticsearch_bulk', size: 100, multisend: false };
        const test = await makeTest(opConfig);
        const results = await test.runSlice([]);

        expect(results).toEqual([]);
    });

    it('does not split if the size is <= than 2 * size in opConfig', async () => {
        // usually each doc is paired with metadata, thus doubling the size of incoming array,
        // hence we double size
        const opConfig = { _op: 'elasticsearch_bulk', size: 50, multisend: false };
        const incData = [];

        for (let i = 0; i < 50; i += 1) {
            incData.push({ index: 'some_index' }, { some: 'data' });
        }

        const test = await makeTest(opConfig);
        const results = await test.runSlice(incData);

        expect(results.length).toEqual(1);
        expect(results[0].body.length).toEqual(100);
    });

    it('does split if the size is greater than 2 * size in opConfig', async () => {
        // usually each doc is paired with metadata, thus doubling the size of incoming array,
        // hence we double size
        const opConfig = { _op: 'elasticsearch_bulk', size: 50, multisend: false };
        const incData = [];

        for (let i = 0; i < 51; i += 1) {
            incData.push({ index: {} }, { some: 'data' });
        }

        const test = await makeTest(opConfig);
        const results = await test.runSlice(incData);

        expect(results.length).toEqual(2);
        expect(results[0].body.length).toEqual(100);
        expect(results[1].body.length).toEqual(2);
    });

    it('splits the array up properly when there are delete operations (not a typical doubling of data)', async () => {
        const opConfig = { _op: 'elasticsearch_bulk', size: 2, multisend: false };
        const incData = [{ create: {} }, { some: 'data' }, { update: {} }, { other: 'data' }, { delete: {} }, { index: {} }, { final: 'data' }];
        const copy = cloneDeep(incData);

        const test = await makeTest(opConfig);
        const results = await test.runSlice(incData);

        expect(results.length).toEqual(2);
        expect(results[0].body).toEqual(copy.slice(0, 5));
        expect(results[1].body).toEqual(copy.slice(5));
    });

    it('multisend will send based off of _id', async () => {
        const opConfig = {
            _op: 'elasticsearch_bulk',
            size: 5,
            multisend: true,
            connection_map: {
                a: 'default',
            }
        };

        const incData = [{ create: { _id: 'abc' } }, { some: 'data' }, { update: { _id: 'abc' } }, { other: 'data' }, { delete: { _id: 'abc' } }, { index: { _id: 'abc' } }, { final: 'data' }];
        const copy = cloneDeep(incData);

        const test = await makeTest(opConfig);
        const results = await test.runSlice(incData);

        expect(results.length).toEqual(1);
        // length to index is off by 1
        expect(results[0].body).toEqual(copy);
    });

    it('will throw if connection_map values do not exists in connector config', async () => {
        expect.hasAssertions();
        const opConfig = {
            _op: 'elasticsearch_bulk',
            size: 5,
            multisend: true,
            connection_map: {
                a: 'NotInConnector'
            }
        };
        const errMsg = 'elasticsearch_bulk connection_map specifies a connection for';
        try {
            await makeTest(opConfig);
        } catch (err) {
            expect(startsWith(err.message, errMsg)).toEqual(true);
        }
    });

    it('can multisend to several places', async () => {
        const opConfig = {
            _op: 'elasticsearch_bulk',
            size: 5,
            multisend: true,
            connection_map: {
                a: 'default',
                b: 'otherConnection'
            }
        };

        const incData = [{ create: { _id: 'abc' } }, { some: 'data' }, { update: { _id: 'abc' } }, { other: 'data' }, { delete: { _id: 'bc' } }, { index: { _id: 'bc' } }, { final: 'data' }];
        const copy = cloneDeep(incData);

        const test = await makeTest(opConfig);
        const results = await test.runSlice(incData);

        expect(results.length).toEqual(2);
        // length to index is off by 1
        expect(results[0].body).toEqual(copy.slice(0, 4));
        expect(results[1].body).toEqual(copy.slice(4));
    });

    it('multisend_index_append will change outgoing _id', async () => {
        const opConfig = {
            _op: 'elasticsearch_bulk',
            size: 5,
            multisend: true,
            multisend_index_append: 'hello',
            connection_map: {
                a: 'default'
            }
        };
        const incData = [
            { create: { _id: 'abc', _index: 'testindex' } },
            { some: 'data' },
            { update: { _id: 'abc', _index: 'testindex' } },
            { other: 'data' },
            { delete: { _id: 'abc', _index: 'testindex' } },
            { index: { _id: 'abc', _index: 'testindex' } },
            { final: 'data' }
        ];

        const finalData = cloneDeep(incData).map((obj: any) => {
            for (const [, value] of Object.entries(obj)) {
                // @ts-ignore
                if (isPlainObject(value)) value._index = `${value._index}-a`;
            }
            return obj;
        });

        const test = await makeTest(opConfig);
        const results = await test.runSlice(incData);

        expect(results.length).toEqual(1);
        // length to index is off by 1
        expect(results[0].body).toEqual(finalData);
    });
});
