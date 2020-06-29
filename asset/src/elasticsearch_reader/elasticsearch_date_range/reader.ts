import {
    Fetcher,
    SliceRequest,
    DataEntity,
    AnyObject,
    APIFactoryRegistry
} from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import { ESDateConfig } from '../interfaces';

type ESReaderFactoryAPI = APIFactoryRegistry<elasticApi.Client, AnyObject>

export default class DateReader extends Fetcher<ESDateConfig> {
    api!: elasticApi.Client;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;

        const apiConfig = this.executionConfig.apis.find((config) => config._name === apiName);
        if (apiConfig == null) throw new Error(`could not find api configuration for api ${apiName}`);
        // TODO: verify this type works
        const apiManager = this.getAPI<ESReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, apiConfig);
    }

    async fetch(slice: SliceRequest): Promise<DataEntity[]> {
        const query = this.api.buildQuery(this.opConfig, slice);
        return this.api.search(query);
    }
}
