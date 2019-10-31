
import {
    Fetcher, AnyObject, SliceRequest, DataEntity
} from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import MockedClient from './client';
import { ApiConfig } from './interfaces';

// TODO: a lot of similarites to ESReader, just differs on client generation

export default class APIReader extends Fetcher<ApiConfig> {
    queryConfig!: AnyObject;
    api!: any;

    async initialize() {
        const client = new MockedClient(this.opConfig, this.logger);
        this.queryConfig = Object.assign({}, this.opConfig, { full_response: true });
        this.api = elasticApi(client, this.logger, this.queryConfig);
    }

    async fetch(slice: SliceRequest) {
        const query = this.api.buildQuery(this.queryConfig, slice);
        const results = await this.api.search(query);
        // TODO: better typeing of doc
        return results.hits.hits.map((doc: any) => {
            const now = Date.now();
            const metadata = {
                _key: doc._id,
                _processTime: now,
                /** @todo this should come from the data */
                _ingestTime: now,
                /** @todo this should come from the data */
                _eventTime: now,
                // pass only the record metadata
                _index: doc._index,
                _type: doc._type,
                _version: doc._version,
            };
            return DataEntity.make(doc._source, metadata);
        });
    }
}
