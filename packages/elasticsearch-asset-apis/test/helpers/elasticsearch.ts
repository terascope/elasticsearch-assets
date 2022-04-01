import { fixMappingRequest, getESVersion } from 'elasticsearch-store';
import { Client, SearchParams, BulkIndexDocumentsParams } from 'elasticsearch';
import {
    DataEntity, AnyObject, debugLogger, pDelay, uniq
} from '@terascope/utils';
import { DataType, LATEST_VERSION, TypeConfigFields } from '@terascope/data-types';
import elasticAPI from '@terascope/elasticsearch-api';
import { ELASTICSEARCH_HOST, ELASTICSEARCH_VERSION } from './config';

const logger = debugLogger('elasticsearch_helpers');

// automatically set the timeout to 10s when using elasticsearch
jest.setTimeout(10000);

export function makeClient(): Client {
    return new Client({
        host: ELASTICSEARCH_HOST,
        log: 'error',
        apiVersion: ELASTICSEARCH_VERSION,
    });
}

export function formatUploadData(
    index: string, type: string, data: any[]
): AnyObject[] {
    const results: any[] = [];

    data.forEach((record) => {
        const meta: any = { _index: index };

        // meta._type = type || '_doc';

        if (DataEntity.isDataEntity(record) && record.getKey()) {
            meta._id = record.getKey();
        }

        results.push({ index: meta }, record);
    });

    return results;
}

export async function upload(
    client: Client, queryBody: BulkIndexDocumentsParams, data: any[]
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
    fields: TypeConfigFields,
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
        refresh: true,
    });

    if (results.errors) {
        const errors: string[] = [];
        for (const response of results.items) {
            if (response.index.error) errors.push(response.index.error.reason);
        }

        throw new Error(`There were errors populating index, errors: ${uniq(errors).join(' ; ')}`);
    }
}

export async function fetch(
    client: Client, query: SearchParams, fullRequest = false
): Promise<(DataEntity[] | DataEntity)> {
    const esClient = elasticAPI(client, logger, { full_response: fullRequest });
    const results = await esClient.search(query);
    return results;
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
            throw new Error(`Could not delete index: ${index}, error: ${err.message}`);
        });

    if (template) {
        await client.indices
            .deleteTemplate({
                name: template,
                requestTimeout: 3000,
            })
            .catch(() => {});
    }
}
