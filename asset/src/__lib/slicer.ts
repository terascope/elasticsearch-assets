import {
    ParallelSlicer,
    SlicerFn,
    AnyObject,
    isFunction,
    SlicerRecoveryData,
} from '@terascope/job-components';
import moment from 'moment';
import { DateSlicerArgs, ElasticsearchReaderAPI } from '@terascope/elasticsearch-asset-apis';
import { ESDateConfig } from '../elasticsearch_reader/interfaces';
import { ElasticReaderFactoryAPI } from '../elasticsearch_reader_api/interfaces';

export default class DateSlicer extends ParallelSlicer<ESDateConfig> {
    protected api!: ElasticsearchReaderAPI;
    protected hasUpdated = false;
    protected startTime = moment().toISOString();

    async initialize(recoveryData: SlicerRecoveryData[]): Promise<void> {
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {});
        // NOTE ORDER MATTERS
        // a parallel slicer initialize calls newSlicer multiple times
        // need to make api before newSlicer is called

        await super.initialize(recoveryData);
    }

    async updateJob(data: AnyObject): Promise<void> {
        const { setMetadata } = this.context.apis.executionContext;

        if (setMetadata && isFunction(setMetadata)) {
            return this.context.apis.executionContext.setMetadata(this.opConfig._op, data);
        }
    }

    async newSlicer(id: number): Promise<SlicerFn> {
        const { lifecycle, slicers } = this.executionConfig;
        const { recoveryData, startTime } = this;

        const hook = async (params: AnyObject) => {
            if (!this.hasUpdated) {
                await this.updateJob(params);
                this.hasUpdated = true;
            }
        };

        const windowState = this.api.makeWindowState(slicers);

        const slicerConfig: DateSlicerArgs = {
            lifecycle,
            numOfSlicers: slicers,
            slicerID: id,
            recoveryData,
            startTime,
            hook,
            windowState
        };

        const slicer = await this.api.makeDateSlicer(slicerConfig);

        return slicer;
    }

    isRecoverable(): boolean {
        return true;
    }
}
