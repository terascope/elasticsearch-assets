import { jest } from '@jest/globals';
import 'jest-extended';
import fs from 'node:fs';
import { v4 as uuidv4 } from 'uuid';
import TerasliceClient, { Job } from 'teraslice-client-js';
import { ASSET_ZIP_PATH, TERASLICE_HOST } from './config.js';
import { Client, ElasticsearchTestHelpers } from '@terascope/opensearch-client';
import { cleanupIndex, makeClient, populateIndex } from '../helpers/index.js';

describe('Elasticsearch Assets e2e', () => {
    jest.setTimeout(60 * 1000);

    let terasliceClient: TerasliceClient;
    let searchClient: Client;
    const indexPrefix = 'e2e-test-';
    const readIndex = `${indexPrefix}read-${uuidv4()}`;
    const writeIndex = `${indexPrefix}write-${uuidv4()}`;
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
        let job: Job;

        beforeAll(async () => {
            job = await terasliceClient.jobs.submit({
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
            });

            await job.waitForStatus('completed');
        });

        it('should be able to successfully process slices', async () => {
            const ex = await job.execution();
            expect(ex._slicer_stats).toBeDefined();
            expect(ex._slicer_stats.processed).toBe(1);
            expect(ex._slicer_stats.failed).toBe(0);
        });

        it('should have reindexed 1000 records', async () => {
            await searchClient.indices.refresh({ index: writeIndex });
            const stats = await searchClient.indices.stats({ index: writeIndex });

            expect(stats._all.total.docs.count).toBe(1000);
        });
    });
});
