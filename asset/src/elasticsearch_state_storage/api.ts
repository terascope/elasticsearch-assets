import { OperationAPI } from '@terascope/job-components';
import { ESCachedStateStorage } from '@terascope/teraslice-state-storage';

export default class ElasticsearchStateStorage extends OperationAPI {
    stateStorage!: ESCachedStateStorage;

    async initialize(): Promise<void> {
        await super.initialize();
        await this.stateStorage.initialize();
    }

    async shutdown(): Promise<void> {
        await super.shutdown();
        await this.stateStorage.shutdown();
    }

    async createAPI(): Promise<ESCachedStateStorage> {
        if (this.stateStorage) {
            return this.stateStorage;
        }

        const { client } = await this.context.apis.foundation.createClient({
            endpoint: this.apiConfig._connection,
            type: 'elasticsearch-next',
            cached: true
        });
        this.stateStorage = new ESCachedStateStorage(client, this.logger, this.apiConfig as any);
        return this.stateStorage;
    }
}
