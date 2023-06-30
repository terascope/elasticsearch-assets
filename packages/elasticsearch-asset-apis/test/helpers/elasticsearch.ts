import { Client, ElasticsearchTestHelpers } from 'elasticsearch-store';
import { ClientParams } from '@terascope/types';
import { DataEntity, debugLogger } from '@terascope/utils';
import elasticAPI from '@terascope/elasticsearch-api';

const logger = debugLogger('elasticsearch_helpers');

// automatically set the timeout to 30s when using elasticsearch
jest.setTimeout(30000);

const {
    makeClient, cleanupIndex, upload,
    populateIndex, waitForData, getTestENVClientInfo
} = ElasticsearchTestHelpers;

export {
    makeClient, cleanupIndex, upload,
    populateIndex, waitForData
};

export function getMajorVersion() {
    const { majorVersion } = getTestENVClientInfo();
    return majorVersion;
}

export async function fetch(
    client: Client, query: ClientParams.SearchParams, fullRequest = false
): Promise<(DataEntity[] | DataEntity)> {
    const esClient = elasticAPI(client, logger, { full_response: fullRequest });
    const results = await esClient.search(query as any);
    return results as any;
}
