import { Fetcher, DataEntity } from '@terascope/job-components';
import { DataFrame } from '@terascope/data-mate';
import { ElasticsearchReaderAPI, ReaderSlice } from '@terascope/elasticsearch-asset-apis';
import { ESDateConfig } from '../elasticsearch_reader/interfaces';
import { ElasticReaderFactoryAPI } from '../elasticsearch_reader_api/interfaces';

export class ReaderAPIFetcher extends Fetcher<ESDateConfig> {
    api!: ElasticsearchReaderAPI;

    async initialize(): Promise<void> {
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {});
        await super.initialize();
    }

    async fetch(slice: ReaderSlice): Promise<DataEntity[] | DataFrame | Buffer> {
        return this.api.fetch(slice);
    }
}
