import { DataFrame } from '@terascope/data-mate';
import {
    DataTypeConfig, ElasticsearchDistribution, FieldType,
    ClientParams, SearchParams
} from '@terascope/types';
import { debugLogger } from '@terascope/utils';
import 'jest-extended';
import nock, { RequestBodyMatcher } from 'nock';
import {
    buildQuery,
    FetchResponseType,
    IDType,
    SpacesAPIConfig,
    SpacesReaderClient
} from '../../src/index.js';

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
            ...overrides
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
            query: 'foo:bar',
            includeTotals: true
        });
        let query: ClientParams.SearchParams;

        beforeEach(async () => {
            query = buildQuery(client.config, {
                count: 100,
            });
        });

        it('should be able to make a search request without use data frames', async () => {
            const params: SearchParams = {
                q: '(foo:bar)',
                size: 100,
                track_total_hits: true
            };
            scope.post(
                `/${index}?token=${token}`,
                params as RequestBodyMatcher
            ).reply(
                200, {
                    results: [{ foo: 'foo', bar: 'bar', byte: 10 }],
                    returning: 1,
                    total: 1000
                });

            const result = await client.search(query, FetchResponseType.data_entities);
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

            const params: SearchParams = {
                q: '(foo:bar)',
                size: 100,
                track_total_hits: true
            };
            scope.post(
                `/${index}?token=${token}&format=dfjson`,
                params as RequestBodyMatcher
            ).reply(
                200, frame.serialize()
            );

            const result = await client.search(
                query,
                FetchResponseType.data_frame,
                dataTypeConfig as any
            );

            expect(result).toBeInstanceOf(DataFrame);
            expect(result.toJSON()).toEqual([
                { foo: 'foo', bar: 'bar', byte: 10 }
            ]);

            expect(result.metadata).toEqual({
                metrics: {
                    total: 1000
                },
            });
        });
    });
});
