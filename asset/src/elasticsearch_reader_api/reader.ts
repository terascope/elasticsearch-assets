import elasticApi from '@terascope/elasticsearch-api';
import { Client } from 'elasticsearch';
import { Logger } from '@terascope/utils';

export default function elasticsearchReader(
    client: Client, logger: Logger, config: elasticApi.Config
): elasticApi.Client {
    return elasticApi(client, logger, config);
}
