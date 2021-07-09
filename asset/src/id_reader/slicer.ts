import {
    ParallelSlicer,
    SlicerFn,
    SlicerRecoveryData
} from '@terascope/job-components';
import { ElasticsearchReaderAPI, IDSlicerConfig } from '@terascope/elasticsearch-asset-apis';
import { ESIDReaderConfig } from './interfaces';
import { ElasticReaderFactoryAPI, ElasticsearchReaderAPIConfig } from '../elasticsearch_reader_api/interfaces';

export default class ESIDSlicer extends ParallelSlicer<ESIDReaderConfig> {
    api!: ElasticsearchReaderAPI;
    version!: number;
    config!: ElasticsearchReaderAPIConfig;

    async initialize(recoveryData: SlicerRecoveryData[]): Promise<void> {
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);

        this.api = await apiManager.create(apiName, {});
        this.version = this.api.version;

        const apiConfig = apiManager.getConfig(apiName);
        if (!apiConfig) throw new Error(`Could not find api config for api_name ${apiName}`);
        this.config = apiConfig;

        if (this.version <= 5 && apiConfig.id_field_name == null) {
            throw new Error('Paramter id_field_name must be set if querying against elasticsearch version >= 6.x');
        }
        // NOTE ORDER MATTERS
        // a parallel slicer initialize calls newSlicer multiple times
        // need to make api before newSlicer is called
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

        const args: IDSlicerConfig = {
            numOfSlicers: slicers,
            slicerID: id,
            recoveryData,
            keyType: key_type,
            keyRange: key_range || undefined,
            startingKeyDepth: starting_key_depth,
            idFieldName: id_field_name || null
        };

        return this.api.makeIDSlicer(args);
    }
}
