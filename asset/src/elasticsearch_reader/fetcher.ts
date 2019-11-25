
import { Fetcher, SliceRequest, getClient } from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import { ESReaderConfig } from './interfaces';
import ESDateFetcher from './elasticsearch_date_range/reader';

export default class ESReader extends Fetcher<ESReaderConfig> {
    esDateFetcher!: ESDateFetcher;

    async initialize() {
        // TODO: opCOnfig does not have proper fields for getClient params
        const client = getClient(this.context, this.opConfig, 'elasticsearch');
        const queryConfig = Object.assign({}, this.opConfig, { full_response: true });
        const api = elasticApi(client, this.logger, queryConfig);
        this.esDateFetcher = new ESDateFetcher(api, queryConfig);
    }

    async fetch(slice: SliceRequest) {
        this.logger.info('slice', { slice });
        return this.esDateFetcher.fetch(slice);
    }
}
