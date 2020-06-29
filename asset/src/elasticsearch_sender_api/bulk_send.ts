import { RouteSenderAPI } from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import {
    isNumber, getTypeOf, DataEntity, isNotNil, isNil, AnyObject
} from '@terascope/utils';
import { SenderConfig } from './interfaces';
import {
    MUTATE_META,
    INDEX_META,
    IndexSpec,
    UpdateConfig
} from '../elasticsearch_index_selector/interfaces';

export default class ElasticsearchSender implements RouteSenderAPI {
    client: elasticApi.Client;
    size: number;
    clientVersion: number;

    constructor(client: elasticApi.Client, config: SenderConfig) {
        const { size } = config;
        if (!isNumber(size)) throw new Error(`Invalid size parameter, expected number, got ${getTypeOf(size)}`);
        this.client = client;
        this.clientVersion = client.getESVersion();
        this.size = size;
    }

    formatBulkData(input: DataEntity[], isFormated = false): AnyObject[] {
        if (isFormated) return input;
        const results: any[] = [];

        input.forEach((record) => {
            const indexingMeta = record.getMetadata(INDEX_META) as IndexSpec;
            if (isNotNil(indexingMeta)) {
                if (this.clientVersion === 7) {
                    results.push(this.sanitizeMeta(indexingMeta));
                } else {
                    results.push(indexingMeta);
                }

                if (isNil(indexingMeta.delete)) {
                    const mutateMeta = record.getMetadata(MUTATE_META) as UpdateConfig;

                    if (isNotNil(mutateMeta)) {
                        results.push(mutateMeta);
                    } else {
                        results.push(record);
                    }
                }
            }
        });

        return results;
    }

    private sanitizeMeta(meta: AnyObject): AnyObject {
        for (const config of Object.values(meta)) {
            config._type = '_doc';
        }
        return meta;
    }

    async send(dataArray: DataEntity[], isFormated = false): Promise<void> {
        const formattedData = this.formatBulkData(dataArray, isFormated);
        const slicedData = splitArray(formattedData, this.size)
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
