import { debugLogger } from '@terascope/job-components';
import { SpacesAPIConfig, IDType } from '../../src';
import RawSpacesClient from '../../src/spaces-api/spaces-client';

describe('Spaces Mock Client', () => {
    const logger = debugLogger('spaces_reader');
    const baseUri = 'http://test.dev';
    const testIndex = 'details-subset';

    it('should look like an elasticsearch client', () => {
        const opConfig: SpacesAPIConfig = {
            index: testIndex,
            endpoint: baseUri,
            token: 'test-token',
            size: 100000,
            interval: '30s',
            delay: '30s',
            date_field_name: 'date',
            timeout: 50,
            start: null,
            end: null,
            preserve_id: false,
            subslice_by_key: false,
            subslice_key_threshold: 50000,
            fields: null,
            key_type: IDType.base64,
            connection: 'default',
            time_resolution: 's',
            type: 'someType',
            starting_key_depth: 0
        };

        const client = new RawSpacesClient(opConfig, logger);

        expect(client.search).toBeDefined();
        expect(client.count).toBeDefined();
        expect(client.cluster).toBeDefined();
        expect(client.cluster.stats).toBeDefined();
        expect(client.cluster.getSettings).toBeDefined();
    });
});
