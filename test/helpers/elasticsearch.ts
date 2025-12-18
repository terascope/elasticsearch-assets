import { Client, ElasticsearchTestHelpers } from '@terascope/opensearch-client';
import { ClientParams } from '@terascope/types';
import { DataEntity, debugLogger, uniq } from '@terascope/core-utils';
import elasticAPI from '@terascope/elasticsearch-api';

const {
    upload, populateIndex, cleanupIndex,
    waitForData, formatUploadData, makeClient
} = ElasticsearchTestHelpers;

const {
    TEST_INDEX_PREFIX = 'es_assets_',
} = process.env;

const logger = debugLogger('elasticsearch_helpers');

export {
    makeClient, formatUploadData, upload,
    populateIndex, waitForData, cleanupIndex,
    TEST_INDEX_PREFIX
};

export async function addToIndex(
    client: Client,
    index: string,
    records: any[],
): Promise<void> {
    const body = formatUploadData(index, records);

    const results = await client.bulk({
        index,
        body,
        refresh: true
    });

    if (results.errors) {
        const errors: string[] = [];
        for (const response of results.items) {
            if (response.index?.error) errors.push(response.index.error.reason);
        }

        throw new Error(`There were errors populating index, errors: ${uniq(errors).join(' ; ')}`);
    }
}

export async function fetch(
    client: Client, query: ClientParams.SearchParams, fullRequest = false
): Promise<(DataEntity[] | DataEntity)> {
    const esClient = elasticAPI(client, logger, { full_response: fullRequest });
    const results = await esClient.search(query as any);
    return results as any;
}
