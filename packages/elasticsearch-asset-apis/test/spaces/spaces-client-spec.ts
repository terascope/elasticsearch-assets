import { DataFrame } from '@terascope/data-mate';
import { DataTypeConfig, FieldType } from '@terascope/types';
import { debugLogger } from '@terascope/utils';
import { SearchParams } from 'elasticsearch';
import 'jest-extended';
import nock from 'nock';
import {
    buildQuery,
    IDType,
    SpacesAPIConfig,
    SpacesReaderClient
} from '../../src';

describe('Spaces Reader Client', () => {
    const baseUri = 'http://test.dev';
    const index = 'test-endpoint';

    const maxSize = 100_000;
    const token = 'test-token';
    const logger = debugLogger('spaces-reader-client');

    const dataTypeConfig: DataTypeConfig = {
        version: 1,
        fields: {
            foo: { type: FieldType.Keyword },
            bar: { type: FieldType.Keyword },
            byte: { type: FieldType.Byte },
        }
    };

    let scope: nock.Scope;

    function newClient(overrides?: Partial<SpacesAPIConfig>): SpacesReaderClient {
        return new SpacesReaderClient({
            endpoint: baseUri,
            token,
            timeout: 2000,
            retry: 0,
            index,
            subslice_by_key: false,
            subslice_key_threshold: 5000,
            starting_key_depth: 0,
            key_type: IDType.base64url,
            time_resolution: 'ms',
            size: maxSize,
            date_field_name: 'created',
            connection: 'default',
            interval: '1m',
            delay: '30s',
            ...overrides,
        }, logger);
    }

    beforeEach(() => {
        scope = nock(baseUri);
    });

    afterEach(() => {
        nock.cleanAll();
    });

    describe('when given a simple request', () => {
        const client = newClient({
            query: 'foo:bar'
        });
        let query: SearchParams;

        beforeEach(async () => {
            query = buildQuery(client.config, {
                count: 100,
            });
        });

        it('should be able to make a search request without use data frames', async () => {
            scope.post(`/${index}?token=${token}`, {
                q: '(foo:bar)',
                size: 100
            }).reply(200, {
                results: [{ foo: 'foo', bar: 'bar', byte: 10 }],
                returning: 1,
                total: 1000
            });

            const result = await client.search(query, false);
            expect(result).toEqual([
                { foo: 'foo', bar: 'bar', byte: 10 }
            ]);
        });

        it('should be able to make a search request with use data frames', async () => {
            const frame = DataFrame.fromJSON(
                dataTypeConfig,
                [{ foo: 'foo', bar: 'bar', byte: 10 }],
                {
                    metadata: {
                        metrics: {
                            total: 1000,
                        }
                    }
                }
            );

            scope.post(`/${index}?token=${token}&format=dfjson`, {
                q: '(foo:bar)',
                size: 100,
            }).reply(200, frame.serialize());

            const result = await client.search(query, true, dataTypeConfig);

            expect(result).toBeInstanceOf(DataFrame);
            expect(result.toJSON()).toEqual([
                { foo: 'foo', bar: 'bar', byte: 10 }
            ]);

            expect(result.metadata).toEqual({
                metrics: {
                    search_time: expect.any(Number),
                    fetched: 1,
                    total: 1000
                },
                search_end_time: expect.any(Number),
            });
        });
    });
});
