import { SliceRequest, AnyObject, DataEntity } from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';

export default class ESReader {
    api: elasticApi.Client;
    queryConfig: AnyObject;

    constructor(api: elasticApi.Client, queryConfig: AnyObject) {
        this.api = api;
        this.queryConfig = queryConfig;
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
