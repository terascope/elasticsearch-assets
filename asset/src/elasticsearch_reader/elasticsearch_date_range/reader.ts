
import { AnyObject, Logger, DataEntity } from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';

export default class Reader {
    queryConfig: AnyObject;
    api: elasticApi.Client;

    constructor(client: any, config: AnyObject, logger: Logger) {
        this.queryConfig = Object.assign({}, config, { full_response: true });
        this.api = elasticApi(client, logger, this.queryConfig);
    }

    async read(slice: any) {
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
