import { isObjectEntity, debugLogger } from '@terascope/utils';
import 'jest-extended';
import connector from '../src';

describe('terafoundation_elasticsearch_connector', () => {
    const logger = debugLogger('connector-test');

    it('has create and config_schema methods', () => {
        expect(connector.config_schema).toBeFunction();
        expect(connector.config_schema).toBeFunction();
    });

    it('config_schema returns a schema', () => {
        const schema = connector.config_schema();

        expect(schema).toBeDefined();
        expect(isObjectEntity(schema)).toBeTrue();
    });

    it('create returns a client and a logger', () => {
        const { client, log } = connector.create({}, logger);

        expect(client).toBeDefined();
        expect(log).toBeDefined();
    });
});
