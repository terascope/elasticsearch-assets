import {
    DataEntity, isString, set, pMap
} from '@terascope/core-utils';
import type { RouteSenderAPI } from '@terascope/job-components';
import { Client, BulkRecord, BulkActionMetadata } from '@terascope/elasticsearch-api';
import {
    ElasticsearchSenderConfig, UpdateConfig
} from './interfaces.js';

export class ElasticsearchBulkSender implements RouteSenderAPI {
    client: Client;
    config: ElasticsearchSenderConfig;
    private isRouter = false;

    constructor(client: Client, config: ElasticsearchSenderConfig) {
        this.client = client;
        this.config = config;
        // _key is the char from router
        if (config._key && isString(config._key)) this.isRouter = true;
    }

    async send(dataArray: Iterable<DataEntity>): Promise<number> {
        let affectedRecords = 0;

        await pMap(
            this.chunkRequests(
                this.createBulkMetadata(dataArray)
            ),
            async (data) => {
                affectedRecords += await this.client.bulkSend(data);
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

    * createBulkMetadata(input: Iterable<DataEntity>): Iterable<BulkRecord> {
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

    private createEsActionMeta(record: DataEntity): BulkRecord {
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

    buildMetadata(record: DataEntity, metaKey = '_key'): Partial<BulkActionMetadata> {
        const meta: Partial<BulkActionMetadata> = {
            _index: this.createRoute(record),
        };

        if (this.config.update_retry_on_conflict && this.config.update_retry_on_conflict > 0) {
            meta.retry_on_conflict = this.config.update_retry_on_conflict;
        }

        const id = record.getMetadata(metaKey);

        if (id) meta._id = id;

        return meta;
    }

    update(
        meta: Partial<BulkActionMetadata>,
        record: DataEntity
    ): BulkRecord {
        const data = this.addUpdateMethod(record);

        if (this.config.upsert) {
            // The upsert field is what is inserted if the key doesn't already exist
            data.upsert = Object.assign({}, record);
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

        data.doc = Object.assign({}, record);

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

    * chunkRequests(dataArray: Iterable<BulkRecord>): Iterable<BulkRecord[]> {
        let i = 0;
        let bulkChunk: BulkRecord[] = [];

        for (const item of dataArray) {
            bulkChunk.push(item);

            if (++i >= this.config.size) {
                yield bulkChunk;
                i = 0;
                bulkChunk = [];
            }
        }

        if (bulkChunk.length) {
            yield bulkChunk;
        }
    }
}
