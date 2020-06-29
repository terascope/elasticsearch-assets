import { debugLogger, AnyObject, DataEntity } from '@terascope/utils';
import { WorkerTestHarness } from 'teraslice-test-harness';
import elasticAPI from '@terascope/elasticsearch-api';
import { makeClient, cleanupIndex, fetch } from '../helpers/elasticsearch';
import { TEST_INDEX_PREFIX, waitForData } from '../helpers';
import Sender from '../../asset/src/elasticsearch_sender_api/bulk_send';

describe('elasticsearch bulk sender module', () => {
    const logger = debugLogger('sender_api_test');
    const client = makeClient();
    const esClient = elasticAPI(client, logger);
    const senderIndex = `${TEST_INDEX_PREFIX}_sender_api_`;
    const type = esClient.getESVersion() === 7 ? '_doc' : 'events';
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

    function createSender(config: AnyObject) {
        const senderConfig = Object.assign({}, { _name: 'test' }, config);
        return new Sender(esClient, senderConfig);
    }

    async function createIndexSelector(opConfig: AnyObject) {
        const config = Object.assign({}, { _op: 'elasticsearch_index_selector' }, opConfig);
        harness = WorkerTestHarness.testProcessor(config);
        await harness.initialize();
        return harness;
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
            const sender = createSender({ size: 100 });
            const data = [{ action: 'index' }];
            const opConfig = {
                index: senderIndex,
                type: 'events',
            };

            const test = await createIndexSelector(opConfig);

            const annotatedData = await test.runSlice(data);

            const [meta, doc] = sender.formatBulkData(annotatedData);

            expect(meta).toEqual({
                index: {
                    _index: 'es_assets__sender_api_',
                    _type: type
                }
            });
            expect(doc).toEqual(data[0]);
        });

        it('can format bulk index data with es7', async () => {
            const sender = createSender({ size: 100 });
            sender.clientVersion = 7;
            const data = [{ action: 'index' }];
            const opConfig = {
                index: senderIndex,
                type: 'events',
            };

            const test = await createIndexSelector(opConfig);

            const annotatedData = await test.runSlice(data);

            const [meta, doc] = sender.formatBulkData(annotatedData);

            expect(meta).toEqual({
                index: {
                    _index: 'es_assets__sender_api_',
                    _type: '_doc'
                }
            });
            expect(doc).toEqual(data[0]);
        });

        it('can format bulk index and preserve id', async () => {
            const sender = createSender({ size: 100 });
            const key = 'foo';
            const obj = DataEntity.make({ action: 'index' });
            obj.setKey(key);
            const data = [obj];
            const opConfig = {
                index: senderIndex,
                preserve_id: true,
                type: 'events',
            };

            const test = await createIndexSelector(opConfig);

            const annotatedData = await test.runSlice(data);

            const [meta, doc] = sender.formatBulkData(annotatedData);

            expect(meta).toEqual({
                index: {
                    _index: 'es_assets__sender_api_',
                    _type: type,
                    _id: key
                }
            });
            expect(doc).toEqual(data[0]);
        });

        it('can format bulk index and set id be field', async () => {
            const sender = createSender({ size: 100 });
            const key = 'foo';
            const data = [{ action: 'index', other: key }];
            const opConfig = {
                index: senderIndex,
                id_field: 'other',
                type: 'events',
            };

            const test = await createIndexSelector(opConfig);

            const annotatedData = await test.runSlice(data);

            const [meta, doc] = sender.formatBulkData(annotatedData);

            expect(meta).toEqual({
                index: {
                    _index: 'es_assets__sender_api_',
                    _type: type,
                    _id: key
                }
            });
            expect(doc).toEqual(data[0]);
        });

        it('can format bulk create data', async () => {
            const sender = createSender({ size: 100 });
            const data = [{ action: 'create' }];
            const opConfig = {
                index: senderIndex,
                create: true,
                type: 'events',
            };

            const test = await createIndexSelector(opConfig);

            const annotatedData = await test.runSlice(data);

            const [meta, doc] = sender.formatBulkData(annotatedData);

            expect(meta).toEqual({
                create: {
                    _index: 'es_assets__sender_api_',
                    _type: type,
                }
            });
            expect(doc).toEqual(data[0]);
        });

        it('can format bulk update data', async () => {
            const sender = createSender({ size: 100 });
            const data = [{ action: 'create' }];
            const opConfig = {
                index: senderIndex,
                update: true,
                type: 'events',
            };

            const test = await createIndexSelector(opConfig);

            const annotatedData = await test.runSlice(data);

            const [meta, doc] = sender.formatBulkData(annotatedData);

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
            const sender = createSender({ size: 100 });
            const data = [{ action: 'create' }];
            const opConfig = {
                index: senderIndex,
                upsert: true,
                type: 'events',
            };

            const test = await createIndexSelector(opConfig);

            const annotatedData = await test.runSlice(data);

            const [meta, doc] = sender.formatBulkData(annotatedData);

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
            const sender = createSender({ size: 100 });
            const key = 'foo';
            const obj = DataEntity.make({ action: 'index' });
            obj.setKey(key);
            const data = [obj];
            const opConfig = {
                index: senderIndex,
                delete: true,
                type: 'events',
            };

            const test = await createIndexSelector(opConfig);

            const annotatedData = await test.runSlice(data);

            const results = sender.formatBulkData(annotatedData);
            expect(results.length).toEqual(1);
            expect(results[0]).toEqual({
                delete: {
                    _index: 'es_assets__sender_api_',
                    _type: type,
                    _id: key
                }
            });
        });
    });

    describe('send', () => {
        it('can bulk send data', async () => {
            const sender = createSender({ size: 100 });
            const data = [{ some: 'data' }, { other: 'data' }];
            const opConfig = {
                index: senderIndex,
                type: 'events',
            };

            const test = await createIndexSelector(opConfig);
            const annotatedData = await test.runSlice(data);

            await sender.send(annotatedData);

            await waitForData(client, senderIndex, 2);

            const query = {
                index: senderIndex,
                q: '*'
            };

            const results = await fetch(client, query);
            expect(results.length).toEqual(2);
        });
    });
});
