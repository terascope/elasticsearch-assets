import { Client, SearchParams, BulkIndexDocumentsParams } from 'elasticsearch';
import { ELASTICSEARCH_HOST, ELASTICSEARCH_API_VERSION } from './config';
import { DataEntity } from '../../../teraslice/packages/utils/dist/src';

// automatically set the timeout to 10s when using elasticsearch
jest.setTimeout(10000);

export function makeClient(): Client {
    return new Client({
        host: ELASTICSEARCH_HOST,
        log: 'error',
        apiVersion: ELASTICSEARCH_API_VERSION,
    });
}

export function formatUploadData(index: string, data: any[], type?: string,) {
    const results: any[] = [];

    data.forEach((record) => {
        const meta: any = { _index: index, _type: type };

        if (DataEntity.isDataEntity(record) && record.getKey()) {
            meta._id = record.getKey();
        }

        results.push({ index: meta }, record);
    });

    return results;
}

export async function upload(client: Client, _query: BulkIndexDocumentsParams, data: any[]) {
    const body = formatUploadData(_query.index as string, data, _query.type);
    const query = Object.assign({ refresh: 'wait_for', body }, _query);
    return client.bulk(query);
}

export async function fetch(client: Client, query: SearchParams, fullRequest = false) {
    const results = await client.search(query);
    if (!fullRequest) return results.hits.hits.map((obj) => obj._source);
    return results;
}

export async function cleanupIndex(client: Client, index: string, template?: string) {
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
