
import Promise from 'bluebird';
import elasticApi from '@terascope/elasticsearch-api';
import {
    getClient,
    BatchProcessor,
    DataEntity,
    WorkerContext,
    ExecutionConfig,
    AnyObject,
    has,
    flatten
} from '@terascope/job-components';
import { BulkSender } from './interfaces';

interface Endpoint {
    client: elasticApi.Client;
    data: any[];
}

interface BulkContexts {
    [key: string]: Endpoint;
}

// TODO: check correct way of getting client
export default class ElasticsearchBulk extends BatchProcessor<BulkSender> {
    limit: number;
    bulkContexts: BulkContexts = {};
    isMultisend: boolean;
    client: elasticApi.Client;
    multisendIndexAppend: boolean;

    constructor(context: WorkerContext, opConfig: BulkSender, exConfig: ExecutionConfig) {
        super(context, opConfig, exConfig);
        const {
            connection_map: connectionMap,
            multisend_index_append: multisendIndexAppend,
            size: limit,
            multisend
        } = opConfig;

        this.limit = limit;
        this.isMultisend = multisend;
        this.multisendIndexAppend = multisendIndexAppend;
        this.client = this._createClient();
        this.bulkContexts = {};

        if (this.isMultisend) {
            for (const keyset of Object.keys(connectionMap)) {
                // TODO: is this missing pertinent keys without rest of opConfig?
                const connection = getClient(context, { connection: connectionMap[keyset] }, 'elasticsearch');
                const client = elasticApi(connection, this.logger, this.opConfig);
                const keys = keyset.split(',');

                for (const key of keys) {
                    this.bulkContexts[key.toLowerCase()] = {
                        client,
                        data: []
                    };
                }
            }
        }
    }

    private _createClient() {
        const client = getClient(this.context, this.opConfig, 'elasticsearch');
        return elasticApi(client, this.logger, this.opConfig);
    }

    private _recursiveSend(client: elasticApi.Client, dataArray: any[]) {
        const slicedData = splitArray(dataArray, this.limit);
        return Promise.map(slicedData, (data) => client.bulkSend(data));
    }

    private async multiSend(data: any[]) {
        for (let i = 0; i < data.length;) {
            const meta = data[i];
            let record = null;
            // If this is a delete operation there will be no associated data record
            if (!meta.delete) {
                record = data[i + 1];
            }

            const realMeta = extractMeta(meta);

            // TODO: to really be general there will need to be some options
            // in how keys are mapped to indices.
            if (realMeta._id) {
                const selector = realMeta._id.charAt(0);

                if (this.multisendIndexAppend) {
                    if (has(this.bulkContexts, selector)) {
                        realMeta._index = `${realMeta._index}-${selector}`;
                    }
                }
                // typically every metadata is paired with the actual data,
                //   except for delete metadata
                if (has(this.bulkContexts, selector)) {
                    this.bulkContexts[selector].data.push(meta);

                    if (record) {
                        this.bulkContexts[selector].data.push(record);
                    }
                } else if (has(this.bulkContexts, '*')) {
                    this.bulkContexts['*'].data.push(meta);

                    if (record) {
                        this.bulkContexts['*'].data.push(record);
                    }
                } else {
                    this.logger.error(`elasticsearch_bulk: invalid connection selector extracted from key: ${realMeta._id}`);
                }
            } else {
                throw new Error('elasticsearch_bulk: multisend is set but records do not have _id in the bulk request input.');
            }

            i += 1; // skip over the metadata
            if (record) i += 1; // And if there is a data record then skip again.
        }

        const senders = [];

        for (const [, { data: dataList, client }] of Object.entries(this.bulkContexts)) {
            if (dataList.length > 0) {
                senders.push(this._recursiveSend(client, dataList));
            }
        }
        const results = await Promise.all(senders);
        return flatten(results);
    }

    async onBatch(data: DataEntity[]) {
        // bulk throws an error if you send an empty array
        if (data == null || data.length === 0) {
            return Promise.resolve(data);
        }

        if (this.isMultisend) {
            return this.multiSend(data);
        }

        return this._recursiveSend(this.client, data);
    }
}

// TODO: better types
function isMeta(meta: AnyObject) {
    if (meta.index) return { type: 'index', realMeta: meta.index };
    if (meta.create) return { type: 'create', realMeta: meta.create };
    if (meta.update) return { type: 'update', realMeta: meta.update };
    if (meta.delete) return { type: 'delete', realMeta: meta.delete };

    return false;
}

function extractMeta(meta: AnyObject) {
    if (meta.index) return meta.index;
    if (meta.create) return meta.create;
    if (meta.update) return meta.update;
    if (meta.delete) return meta.delete;

    throw new Error('elasticsearch_bulk: Unknown elasticsearch operation in bulk request.');
}

function splitArray(dataArray: AnyObject[], splitLimit: number) {
    const docLimit = splitLimit * 2;

    if (dataArray.length > docLimit) {
        const splitResults = [];

        while (dataArray.length) {
            const end = dataArray.length - 1 > docLimit ? docLimit : dataArray.length - 1;
            const isMetaData = isMeta(dataArray[end]);
            if (isMetaData && isMetaData.type !== 'delete') {
                splitResults.push(dataArray.splice(0, end));
            } else {
                splitResults.push(dataArray.splice(0, end + 1));
            }
        }

        return splitResults;
    }

    return [dataArray];
}
