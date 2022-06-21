import 'jest-extended';
import { debugLogger, AnyObject, DataEntity } from '@terascope/utils';
import { WorkerTestHarness } from 'teraslice-test-harness';
import elasticAPI from '@terascope/elasticsearch-api';
import {
    TEST_INDEX_PREFIX, waitForData, cleanupIndex,
    fetch, makeClient
} from '../test/helpers';
import { createElasticsearchBulkSender } from '../src/elasticsearch-bulk-sender';

describe('elasticsearch bulk sender module', () => {
    const META_ROUTE = 'standard:route';
    const logger = debugLogger('sender_api_test');
    const senderIndex = `${TEST_INDEX_PREFIX}_sender_api_`;

    let apiClient: elasticAPI.Client;
    let harness: WorkerTestHarness;
    let client: any;
    let type: string;

    beforeAll(async () => {
        client = await makeClient();
        apiClient = elasticAPI(client, logger);
        type = apiClient.isElasticsearch6() ? 'events' : '_doc';
        await cleanupIndex(client, `${senderIndex}*`);
    });

    afterAll(async () => {
        await cleanupIndex(client, `${senderIndex}*`);
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    function createSender(config: AnyObject = {}) {
        const senderConfig = Object.assign(
            {},
            {
                _name: 'test',
                size: 100,
                index: senderIndex,
                type
            },
            config
        ) as any;
        return createElasticsearchBulkSender({ client: apiClient, config: senderConfig });
    }

    it('can instantiate', async () => {
        const sender = createSender({ size: 100 });

        expect(sender).toHaveProperty('createBulkMetadata');
        expect(sender).toHaveProperty('send');
    });

    describe('can format bulk data', () => {
        it('can format bulk index data', async () => {
            const sender = createSender();
            const docArray = [DataEntity.make({ action: 'index' })];

            const [{ action, data }] = Array.from(sender.createBulkMetadata(docArray));

            expect(action).toEqual({
                index: {
                    _index: 'ts_test__sender_api_',
                    _type: type
                }
            });

            expect(data).toEqual(docArray[0]);
        });

        it('can format bulk index data with es7', async () => {
            const sender = createSender();

            sender.isElasticsearch6 = false;

            const docArray = [DataEntity.make({ action: 'index' })];

            const [{ action, data }] = Array.from(sender.createBulkMetadata(docArray));

            expect(action).toEqual({
                index: {
                    _index: 'ts_test__sender_api_',
                    _type: '_doc'
                }
            });
            expect(data).toEqual(docArray[0]);
        });

        it('can format bulk index and preserve id', async () => {
            const sender = createSender();
            const key = 'foo';
            const obj = DataEntity.make({ action: 'index' });
            obj.setKey(key);
            const dataArray = [obj];

            const [{ action, data }] = Array.from(sender.createBulkMetadata(dataArray));

            expect(action).toEqual({
                index: {
                    _index: 'ts_test__sender_api_',
                    _type: type,
                    _id: key
                }
            });
            expect(data).toEqual(dataArray[0]);
        });

        it('will not by default have dynamic routing', async () => {
            const sender = createSender();
            const route = 'foo';
            const dataArray = [
                DataEntity.make({ action: 'index' }, { [META_ROUTE]: route })
            ];

            const [{ action, data }] = Array.from(sender.createBulkMetadata(dataArray));

            expect(action).toEqual({
                index: {
                    _index: 'ts_test__sender_api_',
                    _type: type,
                }
            });
            expect(data).toEqual(dataArray[0]);
        });

        it('will have dynamic routing if _key is passed in by dynamic router', async () => {
            const route = 'foo';
            const sender = createSender({ _key: '**' });
            const dataArray = [
                DataEntity.make({ action: 'index' }, { 'standard:route': route })
            ];

            const [{ action, data }] = Array.from(sender.createBulkMetadata(dataArray));

            expect(action).toEqual({
                index: {
                    _index: `${senderIndex}-${route}`,
                    _type: type,
                }
            });
            expect(data).toEqual(dataArray[0]);
        });

        it('can format bulk create data', async () => {
            const sender = createSender({ create: true });
            const dataArray = [DataEntity.make({ action: 'create' })];

            const [{ action, data }] = Array.from(sender.createBulkMetadata(dataArray));

            expect(action).toEqual({
                create: {
                    _index: 'ts_test__sender_api_',
                    _type: type,
                }
            });
            expect(data).toEqual(dataArray[0]);
        });

        it('can format bulk update data', async () => {
            const sender = createSender({ update: true });
            const dataArray = [DataEntity.make({ action: 'update' })];

            const [{ action, data }] = Array.from(sender.createBulkMetadata(dataArray));

            expect(action).toEqual({
                update: {
                    _index: 'ts_test__sender_api_',
                    _type: type,
                }
            });

            expect(data).toEqual({
                doc: dataArray[0]
            });
        });

        it('can format bulk upsert data', async () => {
            const sender = createSender({ upsert: true });
            const dataArray = [DataEntity.make({ action: 'update' })];

            const [{ action, data }] = Array.from(sender.createBulkMetadata(dataArray));

            expect(action).toEqual({
                update: {
                    _index: 'ts_test__sender_api_',
                    _type: type,
                }
            });
            expect(data).toEqual({
                upsert: dataArray[0],
                doc: dataArray[0]
            });
        });

        it('can format bulk delete request', async () => {
            const sender = createSender({ delete: true });
            const key = 'foo';
            const obj = DataEntity.make({ action: 'index' });
            obj.setKey(key);
            const dataArray = [obj];

            const [{ action, data }] = Array.from(sender.createBulkMetadata(dataArray));

            expect(data).toBeUndefined();

            expect(action).toEqual({
                delete: {
                    _index: 'ts_test__sender_api_',
                    _type: type,
                    _id: key
                }
            });
        });

        it('can format bulk create and delete data', async () => {
            const sender = createSender({ create: true });
            const data = [
                DataEntity.make(
                    { action: 'create', field: 'a', _key: 'one' },
                    { _delete_id: 'bar1', _key: 'one' }
                ),
                DataEntity.make(
                    { action: 'create', field: 'b', _key: 'two' },
                    { _delete_id: 'bar2', _key: 'two' }
                )
            ];

            const bulkReq = Array.from(sender.createBulkMetadata(data));

            expect(bulkReq.length).toBe(4);

            expect(bulkReq[0].action).toEqual({
                create: {
                    _index: 'ts_test__sender_api_',
                    _type: type,
                    _id: 'one'
                }
            });

            expect(bulkReq[0].data).toEqual(data[0]);

            expect(bulkReq[1].action).toEqual({
                delete: {
                    _index: 'ts_test__sender_api_',
                    _type: type,
                    _id: 'bar1'
                }
            });

            expect(bulkReq[2].action).toEqual({
                create: {
                    _index: 'ts_test__sender_api_',
                    _type: type,
                    _id: 'two'
                }
            });

            expect(bulkReq[2].data).toEqual(data[1]);

            expect(bulkReq[3].action).toEqual({
                delete: {
                    _index: 'ts_test__sender_api_',
                    _type: type,
                    _id: 'bar2'
                }
            });
        });

        it('can upsert specified fields by passing in an array of keys matching the document', async () => {
            const opConfig = {
                index: 'some_index',
                type: 'events',
                upsert: true,
                update_fields: ['name', 'job']
            };

            const dataArray = [
                DataEntity.make({ some: 'data', name: 'someName', job: 'to be awesome!' })
            ];

            const sender = createSender(opConfig);
            const results = Array.from(sender.createBulkMetadata(dataArray));

            const expectedMetadata = { update: { _index: 'some_index', _type: type } };

            const expectedMutateMetadata = {
                upsert: { some: 'data', name: 'someName', job: 'to be awesome!' },
                doc: { name: 'someName', job: 'to be awesome!' }
            };

            expect(results).toBeArrayOfSize(1);

            expect(results[0].action).toMatchObject(expectedMetadata);
            expect(results[0].data).toMatchObject(expectedMutateMetadata);
        });

        it('script file to run as part of an update request', async () => {
            const opConfig = {
                index: 'some_index',
                type: 'events',
                upsert: true,
                update_fields: [],
                script_file: 'someFile',
                script_params: { aKey: 'job' }
            };

            const dataArray = [
                DataEntity.make({ some: 'data', name: 'someName', job: 'to be awesome!' })
            ];

            const sender = createSender(opConfig);
            const results = Array.from(sender.createBulkMetadata(dataArray));

            const expectedMetadata = { update: { _index: 'some_index', _type: type } };

            const expectedMutateMetadata = {
                upsert: { some: 'data', name: 'someName', job: 'to be awesome!' },
                script: { file: 'someFile', params: { aKey: 'to be awesome!' } }
            };

            expect(results).toBeArrayOfSize(1);
            expect(results[0].action).toMatchObject(expectedMetadata);
            expect(results[0].data).toMatchObject(expectedMutateMetadata);
        });

        it('script to run as part of an update request', async () => {
            const opConfig = {
                index: 'hello',
                type: 'events',
                upsert: true,
                update_fields: [],
                script: 'ctx._source.count += add',
                script_params: { add: 'add' }
            };

            const dataArray = [
                DataEntity.make({ count: 1, add: 2 })
            ];
            const sender = createSender(opConfig);
            const results = Array.from(sender.createBulkMetadata(dataArray));

            const expectedMetadata = { update: { _index: 'hello', _type: type } };

            const expectedMutateMetadata = {
                upsert: { count: 1, add: 2 },
                script: {
                    source: 'ctx._source.count += add',
                    params: {
                        add: 2
                    }
                }
            };

            expect(results).toBeArrayOfSize(1);
            expect(results[0].action).toMatchObject(expectedMetadata);
            expect(results[0].data).toMatchObject(expectedMutateMetadata);
        });
    });

    describe('send', () => {
        it('can bulk send data test', async () => {
            const sender = createSender();
            const data = [
                DataEntity.make({ some: 'data' }),
                DataEntity.make({ other: 'data' })
            ];

            await sender.send(data);

            await waitForData(client, senderIndex, 2);

            const query = {
                index: senderIndex,
                q: '*'
            };

            const results = await fetch(client, query);
            expect(results.length).toEqual(2);
        });

        it('can bulk send data to dynamic routes', async () => {
            const route1 = 'a';
            const route2 = 'b';

            const finalRoute1 = `${senderIndex}-${route1}`;
            const finalRoute2 = `${senderIndex}-${route2}`;

            const sender = createSender({ _key: '**', create: true });
            const data = [
                DataEntity.make({ some: 'data' }, { [META_ROUTE]: route1, _key: '1234' }),
                DataEntity.make({ other: 'data' }, { [META_ROUTE]: route2, _key: '5678' })
            ];

            await sender.send(data);

            await Promise.all([
                waitForData(client, finalRoute1, 1),
                waitForData(client, finalRoute2, 1)
            ]);

            const query = {
                index: `${senderIndex}-*`,
                q: '*'
            };

            const results = await fetch(client, query);
            expect(results.length).toEqual(2);

            const meta: string[] = results.map((doc: DataEntity) => doc.getMetadata('_index'));

            expect(meta).toContain(finalRoute1);
            expect(meta).toContain(finalRoute2);
        });
    });
});
