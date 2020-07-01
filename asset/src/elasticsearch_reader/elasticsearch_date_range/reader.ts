import { Fetcher, SliceRequest, DataEntity } from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import { ESDateConfig } from '../interfaces';
import { ElasticReaderFactoryAPI } from '../../elasticsearch_reader_api/interfaces';

export default class DateReader extends Fetcher<ESDateConfig> {
    api!: elasticAPI.Client;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {});
    }

    async fetch(slice: SliceRequest): Promise<DataEntity[]> {
        const query = this.api.buildQuery(this.opConfig, slice);
        return this.api.search(query);
    }
}
