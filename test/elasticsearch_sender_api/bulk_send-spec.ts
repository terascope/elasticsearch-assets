import 'jest-extended';
import { debugLogger, AnyObject, DataEntity } from '@terascope/utils';
import { WorkerTestHarness } from 'teraslice-test-harness';
import elasticAPI from '@terascope/elasticsearch-api';
import { makeClient, cleanupIndex, fetch } from '../helpers/elasticsearch';
import { TEST_INDEX_PREFIX, waitForData } from '../helpers';
import Sender from '../../asset/src/elasticsearch_sender_api/bulk_send';
import Schema from '../../asset/src/elasticsearch_sender_api/schema';

describe('elasticsearch bulk sender module', () => {
    const META_ROUTE = 'standard:route';
    const logger = debugLogger('sender_api_test');
    const client = makeClient();
    const esClient = elasticAPI(client, logger);
    const senderIndex = `${TEST_INDEX_PREFIX}_sender_api_`;
    const type = esClient.getESVersion() === 7 ? '_doc' : 'events';
    const senderSchema = new Schema({} as any, 'api');

    let harness: WorkerTestHarness;

    beforeAll(async () => {
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

        const fullConfig = senderSchema.validate(senderConfig);

        return new Sender(esClient, fullConfig);
    }

    it('can instantiate', async () => {
        const sender = createSender({ size: 100 });

        expect(sender).toBeDefined();
        expect(sender).toHaveProperty('formatBulkData');
        expect(sender).toHaveProperty('send');
        expect(sender).toHaveProperty('verify');
    });

    describe('can format bulk data', () => {
        it('can format bulk index data', async () => {
            const sender = createSender();
            const data = [DataEntity.make({ action: 'index' })];

            const [meta, doc] = sender.formatBulkData(data);

            expect(meta).toEqual({
                index: {
                    _index: 'es_assets__sender_api_',
                    _type: type
                }
            });
            expect(doc).toEqual(data[0]);
        });

        it('can format bulk index data with es7', async () => {
            const sender = createSender();
            sender.clientVersion = 7;
            const data = [DataEntity.make({ action: 'index' })];

            const [meta, doc] = sender.formatBulkData(data);

            expect(meta).toEqual({
                index: {
                    _index: 'es_assets__sender_api_',
                    _type: '_doc'
                }
            });
            expect(doc).toEqual(data[0]);
        });

        it('can format bulk index and preserve id', async () => {
            const sender = createSender();
            const key = 'foo';
            const obj = DataEntity.make({ action: 'index' });
            obj.setKey(key);
            const data = [obj];

            const [meta, doc] = sender.formatBulkData(data);

            expect(meta).toEqual({
                index: {
                    _index: 'es_assets__sender_api_',
                    _type: type,
                    _id: key
                }
            });
            expect(doc).toEqual(data[0]);
        });

        it('will not by default have dynamic routing', async () => {
            const sender = createSender();
            const route = 'foo';
            const data = [
                DataEntity.make({ action: 'index' }, { [META_ROUTE]: route })
            ];

            const [meta, doc] = sender.formatBulkData(data);

            expect(meta).toEqual({
                index: {
                    _index: 'es_assets__sender_api_',
                    _type: type,
                }
            });
            expect(doc).toEqual(data[0]);
        });

        it('will have dynamic routing if _key is passed in by dynamic router', async () => {
            const route = 'foo';
            const sender = createSender({ _key: '**' });
            const data = [
                DataEntity.make({ action: 'index' }, { 'standard:route': route })
            ];

            const [meta, doc] = sender.formatBulkData(data);

            expect(meta).toEqual({
                index: {
                    _index: `${senderIndex}-${route}`,
                    _type: type,
                }
            });
            expect(doc).toEqual(data[0]);
        });

        it('can format bulk create data', async () => {
            const sender = createSender({ create: true });
            const data = [DataEntity.make({ action: 'create' })];

            const [meta, doc] = sender.formatBulkData(data);

            expect(meta).toEqual({
                create: {
                    _index: 'es_assets__sender_api_',
                    _type: type,
                }
            });
            expect(doc).toEqual(data[0]);
        });

        it('can format bulk update data', async () => {
            const sender = createSender({ update: true });
            const data = [DataEntity.make({ action: 'update' })];

            const [meta, doc] = sender.formatBulkData(data);

            expect(meta).toEqual({
                update: {
                    _index: 'es_assets__sender_api_',
                    _type: type,
                }
            });
            expect(doc).toEqual({
                doc: data[0]
            });
        });

        it('can format bulk upsert data', async () => {
            const sender = createSender({ upsert: true });
            const data = [DataEntity.make({ action: 'update' })];

            const [meta, doc] = sender.formatBulkData(data);

            expect(meta).toEqual({
                update: {
                    _index: 'es_assets__sender_api_',
                    _type: type,
                }
            });
            expect(doc).toEqual({
                upsert: data[0],
                doc: data[0]
            });
        });

        it('can format bulk delete request', async () => {
            const sender = createSender({ delete: true });
            const key = 'foo';
            const obj = DataEntity.make({ action: 'index' });
            obj.setKey(key);
            const data = [obj];

            const results = sender.formatBulkData(data);

            expect(results.length).toEqual(1);
            expect(results[0]).toEqual({
                delete: {
                    _index: 'es_assets__sender_api_',
                    _type: type,
                    _id: key
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
            const data = [
                DataEntity.make({ some: 'data', name: 'someName', job: 'to be awesome!' })
            ];
            const sender = createSender(opConfig);
            const results = sender.formatBulkData(data);

            const expectedMetadata = { update: { _index: 'some_index', _type: type } };

            const expectedMutateMetadata = {
                upsert: { some: 'data', name: 'someName', job: 'to be awesome!' },
                doc: { name: 'someName', job: 'to be awesome!' }
            };

            expect(results).toBeArrayOfSize(2);
            expect(results[0]).toMatchObject(expectedMetadata);
            expect(results[1]).toMatchObject(expectedMutateMetadata);
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
            const data = [
                DataEntity.make({ some: 'data', name: 'someName', job: 'to be awesome!' })
            ];
            const sender = createSender(opConfig);
            const results = sender.formatBulkData(data);

            const expectedMetadata = { update: { _index: 'some_index', _type: type } };

            const expectedMutateMetadata = {
                upsert: { some: 'data', name: 'someName', job: 'to be awesome!' },
                script: { file: 'someFile', params: { aKey: 'to be awesome!' } }
            };

            expect(results).toBeArrayOfSize(2);
            expect(results[0]).toMatchObject(expectedMetadata);
            expect(results[1]).toMatchObject(expectedMutateMetadata);
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

            const data = [
                DataEntity.make({ count: 1, add: 2 })
            ];
            const sender = createSender(opConfig);
            const results = sender.formatBulkData(data);

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

            expect(results).toBeArrayOfSize(2);
            expect(results[0]).toMatchObject(expectedMetadata);
            expect(results[1]).toMatchObject(expectedMutateMetadata);
        });
    });

    describe('send', () => {
        it('can bulk send data', async () => {
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
