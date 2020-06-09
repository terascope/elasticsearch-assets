import elasticApi from '@terascope/elasticsearch-api';
import {
    getClient,
    BatchProcessor,
    WorkerContext,
    ExecutionConfig
} from '@terascope/job-components';
import {
    DataEntity,
    AnyObject,
    has,
    TSError
} from '@terascope/utils';
import ElasticsearchSender from '../elasticsearch_sender_api/bulk_send';
import { BulkSender } from './interfaces';

interface Endpoint {
    client: ElasticsearchSender;
    data: DataEntity[];
}

interface BulkContexts {
    [key: string]: Endpoint;
}

export default class ElasticsearchBulk extends BatchProcessor<BulkSender> {
    limit: number;
    bulkContexts: BulkContexts = {};
    isMultisend: boolean;
    client: ElasticsearchSender;
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
                const client = this._createClient({ connection: connectionMap[keyset] });
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

    private _createClient(config: AnyObject = this.opConfig) {
        const client = getClient(this.context, config, 'elasticsearch');
        if (client == null) throw new TSError(`Could not find elasticsearch client for connection: ${this.opConfig.connection}`);
        const esClient = elasticApi(client, this.logger, this.opConfig);
        return new ElasticsearchSender(esClient, config as any);
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
                    this.logger.error(`Invalid connection selector extracted from key: ${realMeta._id}`);
                }
            } else {
                throw new TSError('Input records must have _id metadata on them if elasticsearch_bulk: multisend parameter is set.');
            }

            i += 1; // skip over the metadata
            if (record) i += 1; // And if there is a data record then skip again.
        }

        const senders = [];

        for (const [, { data: dataList, client }] of Object.entries(this.bulkContexts)) {
            if (dataList.length > 0) {
                senders.push(client.send(dataList));
            }
        }

        await Promise.all(senders);
    }

    async onBatch(data: DataEntity[]): Promise<DataEntity[]> {
        // bulk throws an error if you send an empty array
        if (data == null || data.length === 0) {
            return data;
        }

        if (this.isMultisend) {
            const formattedData = this.client.formatBulkData(data);
            await this.multiSend(formattedData);
        } else {
            console.log('what is client', this.client)
            await this.client.send(data);
        }

        return data;
    }
}

function extractMeta(meta: AnyObject) {
    if (meta.index) return meta.index;
    if (meta.create) return meta.create;
    if (meta.update) return meta.update;
    if (meta.delete) return meta.delete;

    throw new TSError('elasticsearch_bulk: Unknown elasticsearch operation in bulk request.');
}
