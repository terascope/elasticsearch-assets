import {
    OperationAPI, isNil, isString
} from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import { SenderApi } from '@terascope/types';
import ElasticsearchSender from './bulk_send';
import { SenderConfig } from './interfaces';

export default class ElasticsearchSenderApi extends OperationAPI {
    async createAPI(config: SenderConfig): Promise<SenderApi> {
        const clientConfig = Object.assign({}, this.apiConfig, config);

        if (isNil(clientConfig.connection) || !isString(clientConfig.connection)) throw new Error('Invalid parameter "connection", must provide a valid connection');

        const { client } = this.context.foundation.getConnection({
            endpoint: clientConfig.connection,
            type: 'elasticsearch',
            cached: true
        });

        const esClient = elasticApi(client, this.context.logger, clientConfig);

        return new ElasticsearchSender(esClient, clientConfig);
    }
}
