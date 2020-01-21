import {
    Fetcher,
    SliceRequest,
    DataEntity,
    WorkerContext,
    ExecutionConfig
} from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import { ESDateConfig } from '../interfaces';

export default class DateReader extends Fetcher<ESDateConfig> {
    api: elasticApi.Client;

    constructor(
        context: WorkerContext,
        opConfig: ESDateConfig,
        executionConfig: ExecutionConfig,
        client: any
    ) {
        super(context, opConfig, executionConfig);
        this.api = elasticApi(client, this.logger, this.opConfig);
    }

    async fetch(slice: SliceRequest) {
        const query = this.api.buildQuery(this.opConfig, slice);
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
