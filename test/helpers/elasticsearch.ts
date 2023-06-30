import { Client, ElasticsearchTestHelpers } from 'elasticsearch-store';
import { ClientParams } from '@terascope/types';
import { DataEntity, debugLogger, uniq } from '@terascope/utils';
import elasticAPI from '@terascope/elasticsearch-api';

const logger = debugLogger('elasticsearch_helpers');

// automatically set the timeout to 30s when using elasticsearch
jest.setTimeout(30000);

const {
    formatUploadData, makeClient, cleanupIndex,
    upload, populateIndex, waitForData,
    removeTypeTest
} = ElasticsearchTestHelpers;

export {
    makeClient, cleanupIndex, upload,
    populateIndex, waitForData, removeTypeTest
};

export async function addToIndex(
    client: Client,
    index: string,
    records: any[],
    type = '_doc'
): Promise<void> {
    const body = formatUploadData(index, records);

    const results = await client.bulk({
        index,
        type,
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
