import {
    ParallelSlicer, SlicerFn, AnyObject,
    isFunction, SlicerRecoveryData,
} from '@terascope/job-components';
import moment from 'moment';
import {
    ElasticsearchReaderAPI, DateSlicerRanges
} from '@terascope/elasticsearch-asset-apis';
import { ESDateConfig } from '../elasticsearch_reader/interfaces.js';
import { ElasticReaderFactoryAPI } from '../elasticsearch_reader_api/interfaces.js';

export class DateReaderAPISlicer extends ParallelSlicer<ESDateConfig> {
    protected api!: ElasticsearchReaderAPI;
    protected hasUpdated = false;
    protected startTime = moment().toISOString();
    slicerRanges!: DateSlicerRanges | undefined;

    async initialize(recoveryData: SlicerRecoveryData[]): Promise<void> {
        // NOTE ORDER MATTERS
        // a parallel slicer initialize calls newSlicer multiple times
        // need to make api before newSlicer is called
        const apiName = this.opConfig.api_name as string;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {});

        const { lifecycle, slicers } = this.executionConfig;
        const { startTime } = this;

        this.slicerRanges = await this.api.makeDateSlicerRanges({
            lifecycle,
            numOfSlicers: slicers,
            recoveryData,
            startTime,
            hook: async (params) => {
                if (!this.hasUpdated) {
                    await this.updateJob(params);
                    this.hasUpdated = true;
                }
            },
        });

        await super.initialize(recoveryData);
    }

    async updateJob(data: AnyObject): Promise<void> {
        const { setMetadata } = this.context.apis.executionContext;

        if (setMetadata && isFunction(setMetadata)) {
            return this.context.apis.executionContext.setMetadata(this.opConfig._op, data);
        }
    }

    async newSlicer(id: number): Promise<SlicerFn> {
        // if it get here there is likely no data for the query
        if (this.slicerRanges == null || this.slicerRanges[id] == null) {
            return async () => null;
        }

        const { lifecycle, slicers } = this.executionConfig;
        const { startTime } = this;

        const windowState = this.api.makeWindowState(slicers);

        return this.api.makeDateSlicerFromRange({
            lifecycle,
            numOfSlicers: slicers,
            slicerID: id,
            startTime,
            windowState,
        }, this.slicerRanges[id]!);
    }

    isRecoverable(): boolean {
        return true;
    }
}
