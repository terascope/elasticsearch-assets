import { fixMappingRequest, getESVersion } from 'elasticsearch-store';
import { Client, SearchParams, BulkIndexDocumentsParams } from 'elasticsearch';
import {
    DataEntity, AnyObject, isNil, debugLogger, pDelay
} from '@terascope/utils';
import { DataType, LATEST_VERSION, TypeConfigFields } from '@terascope/data-types';
import elasticApi from '@terascope/elasticsearch-api';
import { ELASTICSEARCH_HOST, ELASTICSEARCH_VERSION } from './config';

const logger = debugLogger('elasticsearch_helpers');

// automatically set the timeout to 10s when using elasticsearch
jest.setTimeout(10000);

export function makeClient(): Client {
    let apiVersion = ELASTICSEARCH_VERSION;
    if (apiVersion.charAt(0) === '7') apiVersion = '7.x';
    return new Client({
        host: ELASTICSEARCH_HOST,
        log: 'error',
        apiVersion,
    });
}

export function formatUploadData(
    index: string, version:number, data: any[], type?: string
): AnyObject[] {
    const results: any[] = [];

    data.forEach((record) => {
        const meta: any = { _index: index };
        if (version === 6) {
            if (isNil(type)) throw new Error('type must be provided is elasticsearch is version 6');
            meta._type = type;
        }

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
        queryBody.index as string, getESVersion(client), data, queryBody.type
    );
    const query = Object.assign({ refresh: 'wait_for', body }, queryBody);
    return client.bulk(query);
}

export async function populateIndex(
    client: Client,
    index: string,
    fields: TypeConfigFields,
    records: any[]
): Promise<void> {
    const overrides = {
        settings: {
            'index.number_of_shards': 1,
            'index.number_of_replicas': 0,
        },
    };

    const dataType = new DataType({ version: LATEST_VERSION, fields });
    const version = getESVersion(client);
    const mapping = dataType.toESMapping({ typeName: 'events', overrides, version });

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
    const body = formatUploadData(index, version, records, 'events');

    await client.bulk({
        index,
        type: 'events',
        body,
        refresh: true
    });
}

export async function fetch(
    client: Client, query: SearchParams, fullRequest = false
): Promise<(AnyObject[] | AnyObject)> {
    const esClient = elasticApi(client, logger, { full_response: fullRequest });
    const results = await esClient.search(query);
    return results;
}

export async function waitForData(
    client: Client, index: string, count: number, timeout = 5000
): Promise<void> {
    const esClient = elasticApi(client, logger);
    const failTestTime = Date.now() + timeout;

    return new Promise((resolve, reject) => {
        async function checkIndex() {
            if (failTestTime <= Date.now()) reject(new Error('Could not find count in alloated time'));
            await pDelay(100);
            try {
                const indexCount = await esClient.count({ index, q: '*' });
                if (count === indexCount) resolve();
            } catch (err) {
                reject(err);
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
        .delete({
            index,
            requestTimeout: 3000,
        })
        .catch(() => {});

    if (template) {
        await client.indices
            .deleteTemplate({
                name: template,
                requestTimeout: 3000,
            })
            .catch(() => {});
    }
}
