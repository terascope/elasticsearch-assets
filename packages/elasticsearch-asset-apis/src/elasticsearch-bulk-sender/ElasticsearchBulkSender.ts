import { RouteSenderAPI } from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import {
    DataEntity,
    AnyObject,
    isString,
    fastAssign,
    set,
    chunk
} from '@terascope/utils';
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

    private createEsActionMeta(record: DataEntity): BulkAction {
        const meta = this._createActionMeta(record);

        if (this.config.update || this.config.upsert) {
            return this._update(meta, record);
        }

        if (this.config.delete) {
            return { action: { delete: meta } };
        }

        if (this.config.create) {
            return { action: { create: meta }, data: record };
        }

        return { action: { index: meta }, data: record };
    }

    formatBulkData(input: DataEntity[]): AnyObject[] {
        const bulkRequest: any[] = [];

        for (const record of input) {
            const reps = this.createEsActionMeta(record);

            bulkRequest.push(reps);

            // allows for creation of new record and deletion of old record in one pass
            // useful for fixing keying mistakes
            if (record.getMetadata('_delete_id')) {
                bulkRequest.push(
                    {
                        delete: {
                            _id: record.getMetadata('_delete_id'),
                            _index: this.createRoute(record),
                            _type: this.getType()
                        }
                    }
                );
            }
        }

        return bulkRequest;
    }

    async send(dataArray: DataEntity[]): Promise<void> {
        const bulkRequest = this.formatBulkData(dataArray);

        const bulkRequestResponse = splitArray(bulkRequest, this.config.size)
            .map((data: any) => this.client.bulkSend(data));

        await Promise.all(bulkRequestResponse);
    }
    // unknown if needs to be implemented for elasticsearch
    async verify(): Promise<void> {}

    _createActionMeta(record: DataEntity): Partial<BulkMeta> {
        const meta: Partial<BulkMeta> = {
            _index: this.createRoute(record),
            _type: this.getType()
        };

        if (this.config.update_retry_on_conflict && this.config.update_retry_on_conflict > 0) {
            meta.retry_on_conflict = this.config.update_retry_on_conflict;
        }

        const id = record.getMetadata('_key');

        if (id) meta._id = id;

        return meta;
    }

    _update(meta: Partial<BulkMeta>, record: DataEntity): BulkAction {
        const data = this._addUpdateMethod(record);

        if (this.config.upsert) {
            // The upsert field is what is inserted if the key doesn't already exist
            data.upsert = fastAssign({}, record);
        }

        return { action: { update: meta }, data };
    }

    _addUpdateMethod(record: DataEntity): UpdateConfig {
        const data: UpdateConfig = {};

        if (this.config.update_fields && this.config.update_fields.length > 0) {
            return this._applyUpdateFields(data, record);
        }

        if (this.config.script_file || this.config.script) {
            return this._applyScript(data, record);
        }

        data.doc = fastAssign({}, record);

        return data;
    }

    _applyUpdateFields(data: UpdateConfig, record: DataEntity): UpdateConfig {
        data.doc = {};

        this.config.update_fields!.forEach((field) => {
            data.doc![field] = record[field];
        });

        return data;
    }

    _applyScript(data: UpdateConfig, record: DataEntity): UpdateConfig {
        if (this.config.script_file) data.script = { file: this.config.script_file };

        if (this.config.script) data.script = { source: this.config.script };

        set(data, 'script.params', {});

        for (const [key, field] of Object.entries(this.config.script_params ?? {})) {
            if (record[field]) data.script!.params![key] = record[field];
        }

        return data;
    }
}

function splitArray(dataArray: AnyObject[], splitLimit: number) {
    const preppedData = [];

    const chunks = chunk(dataArray, splitLimit);

    for (const c of chunks) {
        const bulkChunk = [];

        for (const i of c) {
            const { data, action } = i;

            bulkChunk.push(action);
            if (data) bulkChunk.push(data);
        }

        preppedData.push(bulkChunk);
    }

    return preppedData;
}
