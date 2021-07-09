import { RouteSenderAPI } from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import {
    DataEntity,
    AnyObject,
    isString,
    fastAssign,
    set
} from '@terascope/utils';
import {
    ElasticsearchSenderConfig, IndexSpec, BulkMeta, UpdateConfig
} from './interfaces';

export class ElasticsearchBulkSender implements RouteSenderAPI {
    client: elasticAPI.Client;
    config: ElasticsearchSenderConfig;
    clientVersion: number;
    private isRouter = false;

    constructor(client: elasticAPI.Client, config: ElasticsearchSenderConfig) {
        this.client = client;
        this.clientVersion = client.getESVersion();
        this.config = config;
        // _key is the char from router
        if (config._key && isString(config._key)) this.isRouter = true;
    }

    private createRoute(record: DataEntity): string {
        let { index } = this.config;
        // we only allow dynamic routes with the router
        // bulk_sender only sends to a given index
        if (this.isRouter) {
            const routeMetadata = record.getMetadata('standard:route');
            if (routeMetadata) index = `${index}-${routeMetadata}`;
        }

        return index;
    }

    private createBulkMeta(record: DataEntity) {
        const indexMeta: IndexSpec = {};
        const index = this.createRoute(record);
        let data: DataEntity | null | UpdateConfig = record;
        const meta: Partial<BulkMeta> = {
            _index: index
        };
        let update: UpdateConfig | null = null;

        if (this.clientVersion < 7 && this.config.type) {
            meta._type = this.config.type;
        } else {
            meta._type = '_doc';
        }

        const id = record.getMetadata('_key');

        if (id) meta._id = id;

        if (this.config.update || this.config.upsert) {
            indexMeta.update = meta;

            if (this.config.update_retry_on_conflict && this.config.update_retry_on_conflict > 0) {
                meta.retry_on_conflict = this.config.update_retry_on_conflict;
            }

            update = {};

            if (this.config.upsert) {
                // The upsert field is what is inserted if the key doesn't already exist
                update.upsert = fastAssign({}, record);
            }

            // This will merge this record with the existing record.
            if (this.config.update_fields && this.config.update_fields.length > 0) {
                update.doc = {};
                this.config.update_fields.forEach((field) => {
                    // @ts-expect-error
                    update.doc[field] = record[field];
                });
            } else if (this.config.script_file || this.config.script) {
                if (this.config.script_file) {
                    update.script = {
                        file: this.config.script_file
                    };
                }

                if (this.config.script) {
                    update.script = {
                        source: this.config.script
                    };
                }

                set(update, 'script.params', {});
                for (const [key, field] of Object.entries(this.config.script_params ?? {})) {
                    if (record[field]) {
                    // @ts-expect-error
                        update.script.params[key] = record[field];
                    }
                }
            } else {
                update.doc = fastAssign({}, record);
            }

            data = update;
        } else if (this.config.delete) {
            indexMeta.delete = meta;
            data = null;
        } else if (this.config.create) {
            indexMeta.create = meta;
        } else {
            indexMeta.index = meta;
        }

        return { indexMeta, data };
    }

    formatBulkData(input: DataEntity[]): AnyObject[] {
        const results: any[] = [];

        for (const record of input) {
            const { indexMeta, data } = this.createBulkMeta(record);
            results.push(indexMeta);
            if (data) results.push(data);
        }

        return results;
    }

    async send(dataArray: DataEntity[]): Promise<void> {
        const formattedData = this.formatBulkData(dataArray);
        const slicedData = splitArray(formattedData, this.config.size)
            .map((data: any) => this.client.bulkSend(data));

        await Promise.all(slicedData);
    }
    // unknown if needs to be implemented for elasticsearch
    async verify(): Promise<void> {}
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

function isMeta(meta: AnyObject) {
    if (meta.index) return { type: 'index', realMeta: meta.index };
    if (meta.create) return { type: 'create', realMeta: meta.create };
    if (meta.update) return { type: 'update', realMeta: meta.update };
    if (meta.delete) return { type: 'delete', realMeta: meta.delete };

    return false;
}
