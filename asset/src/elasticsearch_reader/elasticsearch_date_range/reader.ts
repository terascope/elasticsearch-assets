import { Fetcher, DataEntity } from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import { ESDateConfig, ReaderSlice } from '../interfaces';
import { ElasticReaderFactoryAPI } from '../../elasticsearch_reader_api/interfaces';
import { buildQuery } from './helpers';

export default class DateReader extends Fetcher<ESDateConfig> {
    api!: elasticAPI.Client;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {});
    }

    async fetch(slice: ReaderSlice): Promise<DataEntity[]> {
        const query = buildQuery(this.opConfig, slice);
        return this.api.search(query);
    }
}
