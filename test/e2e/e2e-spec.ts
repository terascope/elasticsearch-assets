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

interface RecoveryDiagnosticArgs {
    label: string;
    job: Job;
    searchClient: Client;
    destIndex: string;
    sourceIndex: string;
    expectedUuids: string[];
    actualCount: number;
    expectedCount: number;
}

function prefixHistogram(values: string[], depth: number): Record<string, number> {
    const hist: Record<string, number> = {};
    for (const value of values) {
        const key = value.slice(0, depth);
        hist[key] = (hist[key] ?? 0) + 1;
    }
    return hist;
}

/**
 * The destination `_id` is the source `uuid` (the fixture sets `_id` from uuid and the
 * bulk sender writes `_key` -> `_id`), so an `mget` by id tells us exactly which records
 * never made it across.
 */
async function findMissingUuids(
    searchClient: Client, index: string, uuids: string[]
): Promise<string[]> {
    const missing: string[] = [];
    const batchSize = 5000;
    for (let i = 0; i < uuids.length; i += batchSize) {
        const ids = uuids.slice(i, i + batchSize);
        const res = await searchClient.mget({
            index,
            _source: false,
            body: { ids }
        });
        for (const doc of res.docs as Array<{ _id: string; found?: boolean }>) {
            if (!doc.found) missing.push(doc._id);
        }
    }
    return missing;
}

/**
 * Logs why a recovered reindex came up short. Only call this on a count mismatch.
 *
 * The prefix histograms are the key signal. `id_reader` slices by uuid key-prefix, so:
 *  - missing uuids clustered under a few prefixes => a slice range was skipped during
 *    recovery (a read/slice-coverage gap);
 *  - missing uuids scattered evenly => the records were read but not written, i.e. a
 *    write-path loss such as silently-rejected bulk items (see the source-presence check
 *    in step 5) rather than a recovery gap.
 */
async function logRecoveryDiagnostics(args: RecoveryDiagnosticArgs): Promise<void> {
    const {
        label, job, searchClient, destIndex, sourceIndex, expectedUuids, actualCount, expectedCount
    } = args;

    const log = (msg: string) => console.error(`[recovery-diagnostics:${label}] ${msg}`);

    log(`doc count mismatch: expected ${expectedCount}, got ${actualCount} `
        + `(diff ${expectedCount - actualCount})`);

    // 1. Recovery execution + slicer stats: did the slicer believe it finished every slice?
    try {
        const ex = await job.execution() as any;
        log(`recovery ex_id=${ex.ex_id} status=${ex._status} failureReason=${ex._failureReason ?? 'none'}`);
        log(`_slicer_stats=${JSON.stringify(ex._slicer_stats)}`);
    } catch (err) {
        log(`could not fetch execution: ${(err as Error).message}`);
    }

    // 2. Slice-level errors recorded in the state store.
    try {
        const errors = await job.errors();
        log(`recorded slice errors: ${errors.length}`);
        if (errors.length > 0) {
            log(`first errors: ${JSON.stringify(errors.slice(0, 5), null, 2)}`);
        }
    } catch (err) {
        log(`could not fetch errors: ${(err as Error).message}`);
    }

    // 3. Cross-check indices.stats against the count API to rule out a refresh/stats race.
    try {
        const countRes = await searchClient.count({ index: destIndex });
        log(`count API reports ${countRes.count} docs (indices.stats reported ${actualCount})`);
    } catch (err) {
        log(`could not run count: ${(err as Error).message}`);
    }

    // 4. Pinpoint exactly which records are missing and how they are distributed.
    try {
        const missing = await findMissingUuids(searchClient, destIndex, expectedUuids);
        log(`missing record count: ${missing.length}`);
        log(`sample missing uuids: ${JSON.stringify(missing.slice(0, 20))}`);
        log(`missing by 1-char prefix: ${JSON.stringify(prefixHistogram(missing, 1))}`);
        log(`missing by 2-char prefix: ${JSON.stringify(prefixHistogram(missing, 2))}`);

        // 5. Confirm the missing records were actually present in the source. If they still
        // exist in `sourceIndex`, recovery read-coverage was fine and the loss is in the
        // write path (e.g. silently-rejected bulk items); a source that is itself short
        // would instead point upstream of this job.
        const sourceCount = await searchClient.count({ index: sourceIndex });
        log(`source index ${sourceIndex} count: ${sourceCount.count} (expected ${expectedCount})`);
        if (missing.length > 0) {
            const stillInSource = await findMissingUuids(searchClient, sourceIndex, missing);
            log(`of ${missing.length} missing-from-dest uuids, ${missing.length - stillInSource.length} `
                + `are present in source and ${stillInSource.length} are absent from source too`);
        }
    } catch (err) {
        log(`could not compute missing records: ${(err as Error).message}`);
    }
}

describe('Elasticsearch Assets e2e', () => {
    jest.setTimeout(5 * 60 * 1000);

    let terasliceClient: TerasliceClient;
    let searchClient: Client;
    const indexPrefix = 'e2e-test-';
    const readIndex = `${indexPrefix}_read-${uuidv4()}`;
    const writeIndex = `${indexPrefix}_write-${uuidv4()}`;
    const writeIndex2 = `${indexPrefix}_write-${uuidv4()}`;

    const evenSpread = ElasticsearchTestHelpers.EvenDateData;
    const finalData: any[] = [];

    for (let i = 0; i < 100; i++) {
        const myData = evenSpread.data.map((item) => ({
            ...item,
            uuid: uuidv4()
        }));
        finalData.push(...myData);
    }

    beforeAll(async () => {
        terasliceClient = new TerasliceClient({ host: TERASLICE_HOST });
        searchClient = await makeClient();

        await cleanupIndex(searchClient, `${indexPrefix}*`);
        await populateIndex(searchClient, readIndex, evenSpread.EvenDataType, finalData);
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
                    time_resolution: 'ms'
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
            expect(ex._slicer_stats.processed).toBe(32);
            expect(ex._slicer_stats.failed).toBe(0);

            await searchClient.indices.refresh({ index: writeIndex });
            const stats = await searchClient.indices.stats({ index: writeIndex });

            expect(stats._all.total.docs.count).toBe(finalData.length);
        });

        it('should be able to recover and continue while using the elasticsearch_reader', async () => {
            const newJobConfig = cloneDeep(jobConfig);
            const newDateIndex = `${indexPrefix}_write-${uuidv4()}`;

            newJobConfig.name = 'elasticsearch-reader (with recovery)';

            newJobConfig.apis![0].size = 1000;

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
            const actualCount = stats._all.total.docs.count;

            if (actualCount !== finalData.length) {
                await logRecoveryDiagnostics({
                    label: 'elasticsearch_reader',
                    job,
                    searchClient,
                    destIndex: newDateIndex,
                    sourceIndex: readIndex,
                    expectedUuids: finalData.map((item) => item.uuid),
                    actualCount,
                    expectedCount: finalData.length,
                });
            }

            expect(actualCount).toBe(finalData.length);
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
                    key_type: 'base64url',
                    id_field_name: 'uuid',
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
            const newJobConfig = cloneDeep(jobConfig);

            job = await terasliceClient.jobs.submit(newJobConfig);

            await job.waitForStatus('completed');

            await searchClient.indices.refresh({ index: writeIndex2 });
            const stats = await searchClient.indices.stats({ index: writeIndex2 });

            expect(stats._all.total.docs.count).toBe(finalData.length);
        });

        it('should support reindexing by hex id', async () => {
            const newJobConfig = cloneDeep(jobConfig);
            const hexIndex = `${indexPrefix}_write-${uuidv4()}`;

            newJobConfig.name = 'reindex by hex id';

            newJobConfig.apis![0].key_type = 'hexadecimal';
            newJobConfig.apis![1].index = hexIndex;

            job = await terasliceClient.jobs.submit(newJobConfig);

            await job.waitForStatus('completed');

            await searchClient.indices.refresh({ index: hexIndex });
            const stats = await searchClient.indices.stats({ index: hexIndex });

            expect(stats._all.total.docs.count).toBe(finalData.length);
        });

        it('should support reindexing by hex id + key_range', async () => {
            const newJobConfig = cloneDeep(jobConfig);
            const hexIndex = `${indexPrefix}_write-${uuidv4()}`;
            const keyRange = ['a', 'b', 'c', 'd', 'e'];

            newJobConfig.name = 'reindex by hex id (range=a..e)';

            newJobConfig.apis![0].key_type = 'hexadecimal';
            newJobConfig.apis![0].key_range = keyRange;

            newJobConfig.apis![1].index = hexIndex;

            job = await terasliceClient.jobs.submit(newJobConfig);

            await job.waitForStatus('completed');

            await searchClient.indices.refresh({ index: hexIndex });
            const stats = await searchClient.indices.stats({ index: hexIndex });

            const expectedCount = finalData
                .filter((item) => keyRange.includes(item.uuid[0]))
                .length;

            expect(stats._all.total.docs.count).toBe(expectedCount);
        });

        it('should be able to recover and continue while using the id_reader', async () => {
            const newJobConfig = cloneDeep(jobConfig);
            const testIndex = `${indexPrefix}_write-${uuidv4()}`;

            newJobConfig.name = 'id-reader (with recovery)';

            newJobConfig.apis![0].size = 500;

            newJobConfig.apis![1].index = testIndex;

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

            await searchClient.indices.refresh({ index: testIndex });
            const stats = await searchClient.indices.stats({ index: testIndex });
            const actualCount = stats._all.total.docs.count;

            if (actualCount !== finalData.length) {
                await logRecoveryDiagnostics({
                    label: 'id_reader',
                    job,
                    searchClient,
                    destIndex: testIndex,
                    sourceIndex: readIndex,
                    expectedUuids: finalData.map((item) => item.uuid),
                    actualCount,
                    expectedCount: finalData.length,
                });
            }

            expect(actualCount).toBe(finalData.length);
        });
    });
});
