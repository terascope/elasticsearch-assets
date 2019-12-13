import { Fetcher, SliceRequest } from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import ESDateFetcher from '../elasticsearch_reader/elasticsearch_date_range/reader';
import MockedClient from './client';
import { ApiConfig } from './interfaces';

// TODO: a lot of similarites to ESReader, just differs on client generation

export default class APIReader extends Fetcher<ApiConfig> {
    esDateFetcher!: ESDateFetcher;

    async initialize() {
        const client = new MockedClient(this.opConfig, this.logger);
        const queryConfig = Object.assign({}, this.opConfig, { full_response: true });
        const api = elasticApi(client, this.logger, queryConfig);
        this.esDateFetcher = new ESDateFetcher(api, queryConfig);
    }

    async fetch(slice: SliceRequest) {
        return this.esDateFetcher.fetch(slice);
    }
}
