import {
    ParallelSlicer,
    SlicerFn,
    SlicerRecoveryData
} from '@terascope/job-components';
import ElasticsearchAPI from '../elasticsearch_reader_api/elasticsearch-api';
import { ESIDReaderConfig } from './interfaces';
import {
    ElasticReaderFactoryAPI,
    IDSlicerArgs
} from '../elasticsearch_reader_api/interfaces';

export default class ESIDSlicer extends ParallelSlicer<ESIDReaderConfig> {
    api!: ElasticsearchAPI;
    version!: number;

    async initialize(recoveryData: SlicerRecoveryData[]): Promise<void> {
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);

        this.api = await apiManager.create(apiName, {});
        this.version = this.api.version;

        const apiConfig = apiManager.getConfig(apiName);
        if (!apiConfig) throw new Error(`Could not find api config for api_name ${apiName}`);

        if (this.version !== 5 && apiConfig.field == null) {
            throw new Error('Paramter field must be set if querying against elasticsearch version >= 6.x');
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
        const { lifecycle, slicers } = this.executionConfig;
        const { key_type, key_range, starting_key_depth } = this.opConfig;
        const { recoveryData } = this;

        const args: IDSlicerArgs = {
            lifecycle,
            numOfSlicers: slicers,
            slicerID: id,
            recoveryData,
            key_type,
            key_range: key_range || undefined,
            starting_key_depth,
        };

        return this.api.makeIDSlicer(args as IDSlicerArgs);
    }
}
