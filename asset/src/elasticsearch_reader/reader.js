'use strict';

const elasticApi = require('@terascope/elasticsearch-api');
const { DataEntity } = require('@terascope/job-components');

function newReader(opConfig, client) {
    const queryConfig = Object.assign({}, opConfig, { full_response: true });

    return (msg, logger) => {
        const elasticsearch = elasticApi(client, logger, queryConfig);
        const query = elasticsearch.buildQuery(queryConfig, msg);

        return elasticsearch.search(query)
            .then((fullResponseObj) => fullResponseObj.hits.hits.map((doc) => {
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
            }));
    };
}

module.exports = newReader;
