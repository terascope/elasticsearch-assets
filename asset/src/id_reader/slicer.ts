import {
    ParallelSlicer, SlicerFn, SlicerRecoveryData
} from '@terascope/job-components';
import { ElasticsearchReaderAPI, IDStartingRanges } from '@terascope/elasticsearch-asset-apis';
import { ESIDReaderConfig } from './interfaces.js';
import { ElasticReaderFactoryAPI, ElasticsearchReaderAPIConfig } from '../elasticsearch_reader_api/interfaces.js';

export default class ESIDSlicer extends ParallelSlicer<ESIDReaderConfig> {
    api!: ElasticsearchReaderAPI;
    config!: ElasticsearchReaderAPIConfig;
    slicerRanges!: IDStartingRanges;

    async initialize(recoveryData: SlicerRecoveryData[]): Promise<void> {
        // NOTE ORDER MATTERS
        // a parallel slicer initialize calls newSlicer multiple times
        // need to make api before newSlicer is called
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);

        this.api = await apiManager.create(apiName, {});

        const apiConfig = apiManager.getConfig(apiName);

        if (!apiConfig) throw new Error(`Could not find api config for api_name ${apiName}`);
        this.config = apiConfig;

        if (!apiConfig.id_field_name) {
            throw new Error('Parameter id_field_name must be set');
        }

        this.slicerRanges = await this.api.makeIDSlicerRanges({
            numOfSlicers: this.executionConfig.slicers,
        });

        await super.initialize(recoveryData);
    }

    isRecoverable(): boolean {
        if (this.executionConfig.lifecycle === 'once') return true;
        return false;
    }

    async newSlicer(id: number): Promise<SlicerFn> {
        return this.api.makeIDSlicerFromRange({
            numOfSlicers: this.executionConfig.slicers,
            slicerID: id,
            recoveryData: this.recoveryData,
        }, this.slicerRanges[id]);
    }
}
