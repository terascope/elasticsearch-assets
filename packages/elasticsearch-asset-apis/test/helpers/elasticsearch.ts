import {
    fixMappingRequest, getESVersion, createClient,
    Client
} from 'elasticsearch-store';
import { ClientParams, DataTypeFields } from '@terascope/types';
import { Client as LegacyClient } from 'elasticsearch';
import {
    DataEntity, AnyObject, debugLogger,
    pDelay, uniq
} from '@terascope/utils';
import { DataType, LATEST_VERSION } from '@terascope/data-types';
import elasticAPI from '@terascope/elasticsearch-api';
import {
    ELASTICSEARCH_HOST, ELASTICSEARCH_API_VERSION,
    OPENSEARCH_HOST, ELASTICSEARCH_VERSION, OPENSEARCH_VERSION
} from './config';

export const isOpensearchTest = process.env.TEST_OPENSEARCH != null;

const semver = isOpensearchTest ? OPENSEARCH_VERSION.split('.') : ELASTICSEARCH_VERSION.split('.');
export const majorVersion = Number(semver[0]);

export const isES8ClientTest = !isOpensearchTest && majorVersion === 8;

const logger = debugLogger('elasticsearch_helpers');

// automatically set the timeout to 30s when using elasticsearch
jest.setTimeout(30000);

export async function makeClient() {
    let host = ELASTICSEARCH_HOST;

    if (process.env.TEST_OPENSEARCH) {
        host = OPENSEARCH_HOST;
    }

    if (process.env.LEGACY_CLIENT != null) {
        return new LegacyClient({
            host,
            log: 'error',
            apiVersion: ELASTICSEARCH_API_VERSION,
        });
    }

    const { client } = await createClient({
        node: host,
    });

    return client as unknown as Client;
}

export function formatUploadData(
    index: string, type: string, data: any[]
): AnyObject[] {
    const results: any[] = [];

    data.forEach((record) => {
        const meta: any = { _index: index };

        if (!isES8ClientTest) {
            meta._type = '_doc';
        }

        if (DataEntity.isDataEntity(record) && record.getKey()) {
            meta._id = record.getKey();
        }

        results.push({ index: meta }, record);
    });

    return results;
}

export async function upload(
    client: Client, queryBody: ClientParams.BulkParams, data: any[]
): Promise<AnyObject> {
    const body = formatUploadData(
        queryBody.index as string, queryBody.type as string, data
    );
    const query = Object.assign({ refresh: 'wait_for', body }, queryBody);
    return client.bulk(query);
}

export async function populateIndex(
    client: Client,
    index: string,
    fields: DataTypeFields,
    records: any[],
    type = '_doc'
): Promise<void> {
    const overrides = {
        settings: {
            'index.number_of_shards': 1,
            'index.number_of_replicas': 0,
        },
    };

    const dataType = new DataType({ version: LATEST_VERSION, fields });
    const version = getESVersion(client);
    const mapping = dataType.toESMapping({ typeName: type, overrides, version });

    await client.indices.create(
        fixMappingRequest(
            client,
            {
                index,
                waitForActiveShards: 'all',
                body: mapping,
            },
            false
        )
    );

    const body = formatUploadData(index, type, records);

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

export async function waitForData(
    client: Client, index: string, count: number, timeout = 5000
): Promise<void> {
    const esClient = elasticAPI(client, logger);
    const failTestTime = Date.now() + timeout;

    return new Promise((resolve, reject) => {
        async function checkIndex() {
            if (failTestTime <= Date.now()) reject(new Error('Could not find count in alloated time'));
            await pDelay(100);
            try {
                const indexCount = await esClient.count({ index, q: '*' });
                if (count === indexCount) return resolve();
            } catch (err) {
                return reject(err);
            }

            checkIndex();
        }

        checkIndex();
    });
}

export async function cleanupIndex(
    client: Client, index: string, template?: string
): Promise<void> {
    await client.indices
        .delete({ index })
        .catch((err) => {
            // ignore index not found exceptions
            const errType = err.meta ? err.meta : err;
            if (errType.statusCode !== 404) {
                throw err;
            }
        });

    if (template) {
        await client.indices
            .deleteTemplate({
                name: template,
            })
            .catch((err) => {
                const errType = err.meta ? err.meta : err;
                if (errType.statusCode !== 404) {
                    throw err;
                }
            });
    }
}
