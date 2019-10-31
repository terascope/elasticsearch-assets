
import {
    Fetcher,
    SliceRequest,
    AnyObject,
    getClient,
    DataEntity
} from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import { ESReaderConfig } from './interfaces';

export default class ESReader extends Fetcher<ESReaderConfig> {
    queryConfig!: AnyObject;
    api!: elasticApi.Client;

    async initialize() {
        // TODO: opCOnfig does not have proper fields for getClient params
        const client = getClient(this.context, this.opConfig, 'elasticsearch');
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
