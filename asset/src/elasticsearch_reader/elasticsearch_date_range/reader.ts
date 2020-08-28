import { Fetcher, DataEntity } from '@terascope/job-components';
import Reader from '../../elasticsearch_reader_api/reader';
import { ESDateConfig, SlicerDateResults } from '../interfaces';
import { ElasticReaderFactoryAPI } from '../../elasticsearch_reader_api/interfaces';

export default class DateReader extends Fetcher<ESDateConfig> {
    api!: Reader;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {});
    }

    async fetch(slice: SlicerDateResults): Promise<DataEntity[]> {
        return this.api.fetch(slice);
    }
}
