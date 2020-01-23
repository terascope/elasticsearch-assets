import { OperationAPI, WorkerContext, ExecutionConfig } from '@terascope/job-components';
import { ESCachedStateStorage } from '@terascope/teraslice-state-storage';
import { ESStateStorageConfig } from './interfaces';

class ElasticsearchStateStorage extends OperationAPI {
    stateStorage: ESCachedStateStorage;

    constructor(
        context: WorkerContext,
        apiConfig: ESStateStorageConfig,
        executionConfig: ExecutionConfig
    ) {
        super(context, apiConfig, executionConfig);
        const { client } = this.context.foundation.getConnection({
            endpoint: this.apiConfig.connection,
            type: 'elasticsearch',
            cached: true
        });
        // @ts-ignore TODO: fix discrepancies
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
