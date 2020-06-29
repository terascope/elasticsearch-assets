import {
    ParallelSlicer,
    SlicerFn,
    TSError,
    SlicerRecoveryData
} from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import idSlicer from './id-slicer';
import { getKeyArray } from './helpers';
import { ESIDReaderConfig, ESIDSlicerArgs } from './interfaces';
import { ElasticReaderFactoryAPI } from '../elasticsearch_reader_api/interfaces';

export default class ESIDSlicer extends ParallelSlicer<ESIDReaderConfig> {
    api!: elasticAPI.Client;

    async initialize(recoveryData: SlicerRecoveryData[]): Promise<void> {
        await super.initialize(recoveryData);
        const apiName = this.opConfig.api_name;

        const apiConfig = this.executionConfig.apis.find((config) => config._name === apiName);
        if (apiConfig == null) throw new Error(`could not find api configuration for api ${apiName}`);
        // TODO: verify this type works
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, apiConfig);
    }

    isRecoverable(): boolean {
        if (this.executionConfig.lifecycle === 'once') return true;
        return false;
    }

    async newSlicer(id: number): Promise<SlicerFn> {
        const baseKeyArray = getKeyArray(this.opConfig);
        // we slice as not to mutate for when this is called again
        const keyArray = this.opConfig.key_range
            ? this.opConfig.key_range.slice()
            : baseKeyArray.slice();

        if (difference(keyArray, baseKeyArray).length > 0) {
            const error = new TSError(`key_range specified for id_reader contains keys not found in: ${this.opConfig.key_type}`);
            return Promise.reject(error);
        }
        const keySet = divideKeyArray(keyArray, this.executionConfig.slicers);

        const args: Partial<ESIDSlicerArgs> = {
            events: this.context.foundation.getEventEmitter(),
            opConfig: this.opConfig,
            executionConfig: this.executionConfig,
            logger: this.logger,
            api: this.api,
            keySet: keySet[id],
        };

        if (this.recoveryData && this.recoveryData.length > 0) {
            // TODO: verify what retryData is
            // real retry of executionContext here, need to reformat retry data
            const parsedRetry = this.recoveryData.map((obj: any) => {
                // regex to get str between # and *
                if (obj.lastSlice) {
                    // eslint-disable-next-line no-useless-escape
                    return obj.lastSlice.key.match(/\#(.*)\*/)[1];
                }
                // some slicers might not have a previous slice, need to start from scratch
                return '';
            })[id];
            args.retryData = parsedRetry;
        }

        return idSlicer(args as ESIDSlicerArgs);
    }
}

function difference(srcArray: string[] | null, valArray: string[]) {
    const results: string[] = [];
    if (!srcArray) return results;

    for (const val of srcArray) {
        if (!valArray.includes(val)) {
            results.push(val);
        }
    }
    return results;
}

function divideKeyArray(keysArray: string[], num: number) {
    const results = [];
    const len = num;

    for (let i = 0; i < len; i += 1) {
        let divideNum = Math.ceil(keysArray.length / len);

        if (i === num - 1) {
            divideNum = keysArray.length;
        }

        results.push(keysArray.splice(0, divideNum));
    }

    return results;
}
