import { Fetcher, DataEntity } from '@terascope/job-components';
import { DataFrame } from '@terascope/data-mate';
import { ElasticsearchReaderAPI, SlicerDateResults } from '@terascope/elasticsearch-asset-apis';
import { ESDateConfig } from '../elasticsearch_reader/interfaces';
import { ElasticReaderFactoryAPI } from '../elasticsearch_reader_api/interfaces';

export default class DateReader extends Fetcher<ESDateConfig> {
    api!: ElasticsearchReaderAPI;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {});
    }

    async fetch(slice: SlicerDateResults): Promise<DataEntity[] | DataFrame> {
        return this.api.fetch(slice);
    }
}
