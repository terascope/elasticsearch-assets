'use strict';

const { OperationAPI } = require('@terascope/job-components');
const { ESCachedStateStorage } = require('@terascope/teraslice-state-storage');

class ElasticsearchStateStorage extends OperationAPI {
    constructor(context, apiConfig, executionConfig) {
        super(context, apiConfig, executionConfig);
        const { client } = this.context.foundation.getConnection({
            endpoint: this.apiConfig.connection,
            type: 'elasticsearch',
            cached: true
        });

        this.stateStorage = new ESCachedStateStorage(client, this.logger, this.apiConfig);
    }

    async initialize() {
        await super.initialize();
        await this.stateStorage.initialize();
    }

    async shutdown() {
        await super.shutdown();
        await this.stateStorage.shutdown();
    }

    async createAPI() {
        return this.stateStorage;
    }
}

module.exports = ElasticsearchStateStorage;
