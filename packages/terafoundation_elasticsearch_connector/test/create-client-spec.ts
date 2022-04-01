import { debugLogger } from '@terascope/utils';
import 'jest-extended';
import connector from '../src';

describe('terafoundation_elasticsearch_connector createClient', () => {
    const logger = debugLogger('create-client-test');

    it('can create a client based off of the cluster it reads', async () => {
        const config = {
            host: 'localhost:9200',
            user: 'admin',
            password: 'admin'
        };
        const { client, log } = await connector.createClient(config, logger);

        expect(client).toBeDefined();
        expect(log).toBeDefined();
    });
});
