import {
    ParallelSlicer,
    SlicerFn,
    SlicerRecoveryData
} from '@terascope/job-components';
import { ElasticsearchReaderAPI, IDSlicerRanges } from '@terascope/elasticsearch-asset-apis';
import { ESIDReaderConfig } from './interfaces';
import { ElasticReaderFactoryAPI, ElasticsearchReaderAPIConfig } from '../elasticsearch_reader_api/interfaces';

export default class ESIDSlicer extends ParallelSlicer<ESIDReaderConfig> {
    api!: ElasticsearchReaderAPI;
    version!: number;
    config!: ElasticsearchReaderAPIConfig;
    slicerRanges!: IDSlicerRanges;

    async initialize(recoveryData: SlicerRecoveryData[]): Promise<void> {
        // NOTE ORDER MATTERS
        // a parallel slicer initialize calls newSlicer multiple times
        // need to make api before newSlicer is called
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);

        this.api = await apiManager.create(apiName, {});
        this.version = this.api.version;

        const apiConfig = apiManager.getConfig(apiName);
        if (!apiConfig) throw new Error(`Could not find api config for api_name ${apiName}`);
        this.config = apiConfig;

        if (this.version >= 6 && !apiConfig.id_field_name) {
            throw new Error('Paramter id_field_name must be set if querying against elasticsearch version >= 6.x');
        }

        const {
            key_type,
            key_range,
        } = this.config;

        this.slicerRanges = this.api.makeIDSlicerRanges({
            numOfSlicers: this.executionConfig.slicers,
            keyType: key_type,
            keyRange: key_range || undefined,
        });
        await super.initialize(recoveryData);
    }

    isRecoverable(): boolean {
        if (this.executionConfig.lifecycle === 'once') return true;
        return false;
    }

    async newSlicer(id: number): Promise<SlicerFn> {
        const { slicers } = this.executionConfig;

        const {
            key_type,
            key_range,
            starting_key_depth,
            id_field_name
        } = this.config;

        const { recoveryData } = this;

        return this.api.makeIDSlicerFromRange({
            numOfSlicers: slicers,
            slicerID: id,
            recoveryData,
            keyType: key_type,
            keyRange: key_range || undefined,
            startingKeyDepth: starting_key_depth,
            idFieldName: id_field_name || null
        }, this.slicerRanges[id]);
    }
}
