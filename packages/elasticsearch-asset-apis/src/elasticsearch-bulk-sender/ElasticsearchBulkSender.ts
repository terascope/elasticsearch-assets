import {
    RouteSenderAPI,
    DataEntity,
    AnyObject,
    isString,
    fastAssign,
    set,
    pMap
} from '@terascope/utils';
import elasticAPI from '@terascope/elasticsearch-api';
import {
    ElasticsearchSenderConfig, BulkMeta, UpdateConfig, BulkAction
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

    async send(dataArray: Iterable<DataEntity>): Promise<number> {
        let affectedRecords = 0;

        await pMap(
            this.createBulkRequest(
                this.createBulkMetadata(dataArray)
            ),
            async (data) => {
                const result = await this.client.bulkSend(data);
                if (Array.isArray(result.items)) {
                    affectedRecords += result.items.length;
                }
            }
        );

        return affectedRecords;
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

    private getType(): string {
        if (this.clientVersion < 7 && this.config.type) {
            return this.config.type;
        }

        return '_doc';
    }

    * createBulkMetadata(input: Iterable<DataEntity>): Iterable<BulkAction> {
        for (const record of input) {
            yield this.createEsActionMeta(record);

            // allows for creation of new record and deletion of old record in one pass
            // useful for fixing keying mistakes
            if (record.getMetadata('_delete_id')) {
                yield {
                    action: {
                        delete: this.buildMetadata(record, '_delete_id')
                    }
                };
            }
        }
    }

    private createEsActionMeta(record: DataEntity): BulkAction {
        const meta = this.buildMetadata(record);

        if (this.config.update || this.config.upsert) {
            return this.update(meta, record);
        }

        if (this.config.delete) {
            return { action: { delete: meta } };
        }

        if (this.config.create) {
            return { action: { create: meta }, data: record };
        }

        return { action: { index: meta }, data: record };
    }

    buildMetadata(record: DataEntity, metaKey = '_key'): Partial<BulkMeta> {
        const meta: Partial<BulkMeta> = {
            _index: this.createRoute(record),
            _type: this.getType()
        };

        if (this.config.update_retry_on_conflict && this.config.update_retry_on_conflict > 0) {
            meta.retry_on_conflict = this.config.update_retry_on_conflict;
        }

        const id = record.getMetadata(metaKey);

        if (id) meta._id = id;

        return meta;
    }

    update(meta: Partial<BulkMeta>, record: DataEntity): BulkAction {
        const data = this.addUpdateMethod(record);

        if (this.config.upsert) {
            // The upsert field is what is inserted if the key doesn't already exist
            data.upsert = fastAssign({}, record);
        }

        return { action: { update: meta }, data };
    }

    addUpdateMethod(record: DataEntity): UpdateConfig {
        const data: UpdateConfig = {};

        if (this.config.update_fields && this.config.update_fields.length > 0) {
            return this.applyUpdateFields(data, record);
        }

        if (this.config.script_file || this.config.script) {
            return this.applyScript(data, record);
        }

        data.doc = fastAssign({}, record);

        return data;
    }

    applyUpdateFields(data: UpdateConfig, record: DataEntity): UpdateConfig {
        data.doc = {};

        this.config.update_fields!.forEach((field) => {
            data.doc![field] = record[field];
        });

        return data;
    }

    applyScript(data: UpdateConfig, record: DataEntity): UpdateConfig {
        if (this.config.script_file) data.script = { file: this.config.script_file };

        if (this.config.script) data.script = { source: this.config.script };

        set(data, 'script.params', {});

        for (const [key, field] of Object.entries(this.config.script_params ?? {})) {
            if (record[field]) data.script!.params![key] = record[field];
        }

        return data;
    }

    * createBulkRequest(dataArray: Iterable<BulkAction>): Iterable<AnyObject[]> {
        let i = 0;
        let bulkChunk: AnyObject[] = [];

        for (const item of dataArray) {
            const { data, action } = item;

            bulkChunk.push(action);
            if (data) bulkChunk.push(data);

            if (++i >= this.config.size) {
                yield bulkChunk;
                bulkChunk = [];
            }
        }

        if (bulkChunk.length) {
            yield bulkChunk;
        }
    }
}
