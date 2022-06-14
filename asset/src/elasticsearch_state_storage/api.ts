import { OperationAPI } from '@terascope/job-components';
import { ESCachedStateStorage } from '@terascope/teraslice-state-storage';

export default class ElasticsearchStateStorage extends OperationAPI {
    stateStorage!: ESCachedStateStorage;

    async initialize(): Promise<void> {
        const { client } = await this.context.apis.foundation.createClient({
            endpoint: this.apiConfig.connection,
            type: 'elasticsearch-next',
            cached: true
        });
        await super.initialize();

        this.stateStorage = new ESCachedStateStorage(client, this.logger, this.apiConfig as any);
        await this.stateStorage.initialize();
    }

    async shutdown(): Promise<void> {
        await super.shutdown();
        await this.stateStorage.shutdown();
    }

    async createAPI(): Promise<ESCachedStateStorage> {
        return this.stateStorage;
    }
}
