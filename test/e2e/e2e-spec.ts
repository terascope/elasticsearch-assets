import { jest } from '@jest/globals';
import 'jest-extended';
import fs from 'node:fs';
import { v4 as uuidv4 } from 'uuid';
import TerasliceClient, { Job } from 'teraslice-client-js';
import { Client, ElasticsearchTestHelpers } from '@terascope/opensearch-client';
import { Teraslice } from '@terascope/types';
import { cloneDeep } from '@terascope/core-utils';
import { ASSET_ZIP_PATH, TERASLICE_HOST } from './config.js';

const {
    cleanupIndex, populateIndex, makeClient,
} = ElasticsearchTestHelpers;

describe('Elasticsearch Assets e2e', () => {
    jest.setTimeout(60 * 1000);

    let terasliceClient: TerasliceClient;
    let searchClient: Client;
    const indexPrefix = 'e2e-test-';
    const readIndex = `${indexPrefix}_read-${uuidv4()}`;
    const writeIndex = `${indexPrefix}_write-${uuidv4()}`;
    const writeIndex2 = `${indexPrefix}_write-${uuidv4()}`;

    const evenSpread = ElasticsearchTestHelpers.EvenDateData;

    beforeAll(async () => {
        terasliceClient = new TerasliceClient({ host: TERASLICE_HOST });
        searchClient = await makeClient();

        await cleanupIndex(searchClient, `${indexPrefix}*`);
        await populateIndex(searchClient, readIndex, evenSpread.EvenDataType, evenSpread.data);
    });

    afterAll(async () => {
        await cleanupIndex(searchClient, `${indexPrefix}*`);
    });

    describe('asset upload', () => {
        it('should upload the asset bundle', async () => {
            const result = await terasliceClient.assets.upload(
                fs.createReadStream(ASSET_ZIP_PATH)
            );

            expect(result.asset_id).toBeDefined();
        });

        it('should be discoverable on the cluster after upload', async () => {
            const records = await terasliceClient.assets.getAsset('elasticsearch');
            expect(records).not.toBeEmpty();
            expect(records[0].name).toBe('elasticsearch');
        });
    });

    describe('elasticsearch_reader -> elasticsearch_bulk', () => {
        const jobConfig: Teraslice.JobConfigParams = {
            name: 'e2e-elasticsearch-reindex',
            lifecycle: 'once',
            workers: 1,
            assets: ['elasticsearch'],
            operations: [
                {
                    _op: 'elasticsearch_reader',
                    _api_name: 'elasticsearch_reader_api'
                },
                {
                    _op: 'elasticsearch_bulk',
                    _api_name: 'elasticsearch_sender_api'
                }
            ],
            apis: [
                {
                    _name: 'elasticsearch_reader_api',
                    index: readIndex,
                    date_field_name: 'created',
                },
                {
                    _name: 'elasticsearch_sender_api',
                    index: writeIndex
                }
            ]
        };
        let job: Job;

        it('should be able to successfully process slices', async () => {
            job = await terasliceClient.jobs.submit(jobConfig);

            await job.waitForStatus('completed');
            const ex = await job.execution();

            expect(ex._slicer_stats).toBeDefined();
            expect(ex._slicer_stats.processed).toBe(1);
            expect(ex._slicer_stats.failed).toBe(0);

            await searchClient.indices.refresh({ index: writeIndex });
            const stats = await searchClient.indices.stats({ index: writeIndex });

            expect(stats._all.total.docs.count).toBe(1000);
        });

        it('should be able to recover and continue while using the elasticsearch_reader', async () => {
            const newJobConfig = cloneDeep(jobConfig);
            const newDateIndex = `${indexPrefix}_write-${uuidv4()}`;

            newJobConfig.name = 'elasticsearch-reader (with recovery)';

            newJobConfig.apis![0].size = 100;

            newJobConfig.apis![1].index = newDateIndex;

            job = await terasliceClient.jobs.submit(newJobConfig);
            await job.waitForStatus('running');

            await job.pause();
            await job.waitForStatus('paused');

            await job.resume();
            await job.waitForStatus('running');

            await job.stop();
            await job.waitForStatus('stopped');

            await job.recover();
            await job.waitForStatus('completed');

            await searchClient.indices.refresh({ index: newDateIndex });
            const stats = await searchClient.indices.stats({ index: newDateIndex });

            expect(stats._all.total.docs.count).toBe(1000);
        });
    });

    describe('id_reader -> elasticsearch_bulk', () => {
        const jobConfig: Teraslice.JobConfigParams = {
            name: 'ID_Reindex',
            lifecycle: 'once',
            slicers: 2,
            workers: 4,
            assets: ['elasticsearch'],
            apis: [
                {
                    _name: 'elasticsearch_reader_api',
                    index: readIndex,
                    size: 500,
                    key_type: 'base64url'
                },
                {
                    _name: 'elasticsearch_sender_api',
                    index: writeIndex2,
                    size: 200
                }
            ],
            operations: [
                {
                    _op: 'id_reader',
                    _api_name: 'elasticsearch_reader_api',
                },
                {
                    _op: 'elasticsearch_bulk',
                    _api_name: 'elasticsearch_sender_api'
                }
            ]
        };

        let job: Job;

        it('should support reindexing', async () => {
            job = await terasliceClient.jobs.submit(jobConfig);

            await job.waitForStatus('completed');

            await searchClient.indices.refresh({ index: writeIndex2 });
            const stats = await searchClient.indices.stats({ index: writeIndex2 });

            expect(stats._all.total.docs.count).toBe(1000);
        });

        it('should support reindexing by hex id', async () => {
            const newJobConfig = cloneDeep(jobConfig);
            const hexIndex = `${indexPrefix}_write-${uuidv4()}`;

            newJobConfig.name = 'reindex by hex id';

            newJobConfig.apis![0].key_type = 'hexadecimal';
            newJobConfig.apis![1].index = hexIndex;

            job = await terasliceClient.jobs.submit(jobConfig);

            await job.waitForStatus('completed');

            await searchClient.indices.refresh({ index: hexIndex });
            const stats = await searchClient.indices.stats({ index: hexIndex });

            expect(stats._all.total.docs.count).toBe(1000);
        });

        it('should support reindexing by hex id + key_range', async () => {
            const newJobConfig = cloneDeep(jobConfig);
            const hexIndex = `${indexPrefix}_write-${uuidv4()}`;

            newJobConfig.name = 'reindex by hex id (range=a..e)';

            newJobConfig.apis![0].key_type = 'hexadecimal';
            newJobConfig.apis![0].key_range = ['a', 'b', 'c', 'd', 'e'];

            newJobConfig.apis![1].index = hexIndex;

            job = await terasliceClient.jobs.submit(newJobConfig);

            await job.waitForStatus('completed');

            await searchClient.indices.refresh({ index: hexIndex });
            const stats = await searchClient.indices.stats({ index: hexIndex });

            expect(stats._all.total.docs.count).toBe(500);
        });

        it('should be able to recover and continue while using the id_reader', async () => {
            const newJobConfig = cloneDeep(jobConfig);
            const hexIndex = `${indexPrefix}_write-${uuidv4()}`;

            newJobConfig.name = 'id-reader (with recovery)';

            newJobConfig.apis![0].key_type = 'hexadecimal';
            newJobConfig.apis![0].size = 50;

            newJobConfig.apis![1].index = hexIndex;

            job = await terasliceClient.jobs.submit(newJobConfig);
            await job.waitForStatus('running');

            await job.pause();
            await job.waitForStatus('paused');

            await job.resume();
            await job.waitForStatus('running');

            await job.stop();
            await job.waitForStatus('stopped');

            await job.recover();
            await job.waitForStatus('completed');

            await searchClient.indices.refresh({ index: hexIndex });
            const stats = await searchClient.indices.stats({ index: hexIndex });

            expect(stats._all.total.docs.count).toBe(1000);
        });
    });
});
