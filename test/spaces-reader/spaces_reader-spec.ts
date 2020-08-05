import 'jest-extended';
import nock from 'nock';
import path from 'path';
import moment from 'moment';
import { newTestJobConfig, debugLogger, SlicerRecoveryData } from '@terascope/job-components';
import { WorkerTestHarness, SlicerTestHarness } from 'teraslice-test-harness';
import SpacesClient from '../../asset/src/spaces_reader_api/client';
import { ApiConfig } from '../../asset/src/elasticsearch_reader/interfaces';
import { IDType } from '../../asset/src/id_reader/interfaces';
import MockClient from '../mock_client';

describe('spaces_reader', () => {
    const baseUri = 'http://test.dev';
    const testIndex = 'details-subset';
    const logger = debugLogger('spaces_reader');
    const assetDir = path.join(__dirname, '../..');
    let clients: any;
    let defaultClient: MockClient;

    interface EventHook {
        event: string;
        fn: (event?: any) => void;
    }

    beforeEach(() => {
        defaultClient = new MockClient();
        clients = [
            {
                type: 'elasticsearch',
                endpoint: 'default',
                create: () => ({
                    client: defaultClient
                }),
            }
        ];
    });

    let scope: nock.Scope;
    beforeEach(() => {
        scope = nock(baseUri);
    });

    afterEach(() => {
        nock.cleanAll();
    });

    it('should look like an elasticsearch client', () => {
        const opConfig: ApiConfig = {
            _op: 'spaces_reader',
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
            api_name: 'someName'
        };
        const client = new SpacesClient(opConfig, logger);

        expect(client.search).toBeDefined();
        expect(client.count).toBeDefined();
        expect(client.cluster).toBeDefined();
        expect(client.cluster.stats).toBeDefined();
        expect(client.cluster.getSettings).toBeDefined();
    });

    describe('when testing the Fetcher', () => {
        describe.each([
            ['range query', {
                query: {
                    token: 'test-token',
                    q: 'date:[2017-09-23T18:07:14.332Z TO 2017-09-25T18:07:14.332Z}',
                    size: 100
                },
                opConfig: {
                    token: 'test-token',
                    size: 100000,
                    interval: '30s',
                    delay: '30s',
                    date_field_name: 'date',
                },
                msg: {
                    count: 100,
                    start: '2017-09-23T18:07:14.332Z',
                    end: '2017-09-25T18:07:14.332Z',
                }
            }],
            ['lucene query', {
                query: {
                    token: 'test-token',
                    q: '(foo:bar)',
                    size: 5000,
                },
                opConfig: {
                    query: 'foo:bar',
                    token: 'test-token',
                    size: 100000,
                    date_field_name: 'date',
                },
                msg: {
                    count: 5000,
                }
            }],
            ['lucene query with url characters', {
                query: {
                    token: 'test-token',
                    q: '(foo:"bar+baz")',
                    size: 5000,
                },
                opConfig: {
                    query: 'foo:"bar+baz"',
                    token: 'test-token',
                    size: 100000,
                    date_field_name: 'date',
                },
                msg: {
                    count: 5000,
                }
            }],
            ['lucene query with fields', {
                query: {
                    token: 'test-token',
                    q: '(test:query OR other:thing AND bytes:>=2000)',
                    size: 100,
                    fields: 'foo,bar,date'
                },
                opConfig: {
                    query: 'test:query OR other:thing AND bytes:>=2000',
                    token: 'test-token',
                    size: 100000,
                    date_field_name: 'date',
                    fields: ['foo', 'bar'],
                },
                msg: {
                    count: 100,
                }
            }],
            ['lucene query with date range', {
                query: {
                    token: 'test-token',
                    q: 'example_date:[2017-09-23T18:07:14.332Z TO 2017-09-25T18:07:14.332Z} AND (foo:bar)',
                    size: 200,
                },
                opConfig: {
                    query: 'foo:bar',
                    token: 'test-token',
                    size: 100000,
                    date_field_name: 'example_date',
                },
                msg: {
                    count: 200,
                    start: '2017-09-23T18:07:14.332Z',
                    end: '2017-09-25T18:07:14.332Z'
                }
            }],
            ['lucene query with geo point query', {
                query: {
                    token: 'other-token',
                    q: '(foo:bar)',
                    size: 100,
                    geo_point: '52.3456,79.6784',
                    geo_distance: '200km'
                },
                opConfig: {
                    query: 'foo:bar',
                    token: 'other-token',
                    size: 100000,
                    date_field_name: 'date',
                    geo_field: 'some_field',
                    geo_distance: '200km',
                    geo_point: '52.3456,79.6784',
                },
                msg: {
                    count: 100
                }
            }],
            ['lucene query with geo bounding box query', {
                query: {
                    token: 'other-token',
                    q: '(foo:bar)',
                    size: 100000,
                    geo_box_top_left: '34.5234,79.42345',
                    geo_box_bottom_right: '54.5234,80.3456',
                    geo_sort_point: '52.3456,79.6784'
                },
                opConfig: {
                    query: 'foo:bar',
                    token: 'other-token',
                    size: 100000,
                    date_field_name: 'date',
                    geo_field: 'some_field',
                    geo_box_top_left: '34.5234,79.42345',
                    geo_box_bottom_right: '54.5234,80.3456',
                    geo_sort_point: '52.3456,79.6784',
                },
                msg: {}
            }],

        ])('when performing a %s', (m, { query, opConfig: _opConfig, msg }) => {
            const opConfig = Object.assign({
                _op: 'spaces_reader',
                index: testIndex,
                endpoint: baseUri,
                interval: '30s',
                delay: '30s',
                timeout: 50
            }, _opConfig);

            const harness = new WorkerTestHarness(newTestJobConfig({
                name: 'simple-api-reader-job',
                operations: [
                    opConfig,
                    {
                        _op: 'noop'
                    }
                ]
            }), {});

            beforeEach(async () => {
                scope.get(`/${testIndex}`)
                    .query(query)
                    .reply(200, {
                        results: [{ some: 'data' }],
                        total: 1
                    });

                await harness.initialize();
            });

            afterEach(async () => {
                await harness.shutdown();
            });

            it('should make the request', async () => {
                const results = await harness.runSlice(msg);
                expect(results).toBeArrayOfSize(1);
                expect(scope.isDone()).toBeTrue();
            });
        });

        describe('when the request times out', () => {
            const harness = new WorkerTestHarness(newTestJobConfig({
                name: 'simple-api-reader-job',
                max_retries: 0,
                operations: [
                    {
                        _op: 'spaces_reader',
                        query: 'test:query',
                        index: testIndex,
                        endpoint: baseUri,
                        token: 'test-token',
                        size: 100000,
                        interval: '30s',
                        delay: '30s',
                        date_field_name: 'date',
                        timeout: 75
                    },
                    {
                        _op: 'noop'
                    }
                ]
            }), {});

            beforeEach(async () => {
                scope.get(`/${testIndex}`)
                    .query({
                        token: 'test-token',
                        q: '(test:query)',
                        size: 5000,
                    })
                    .delay(500)
                    .reply(200, {
                        results: [{ some: 'data' }],
                        total: 1
                    });

                await harness.initialize();
            });

            afterEach(async () => {
                await harness.shutdown();
            });

            it('should throw a timeout error', async () => {
                try {
                    await harness.runSlice({ count: 5000 });
                    throw new Error('Expected slice to fail');
                } catch (err) {
                    expect(err.message).toInclude('HTTP request timed out connecting to API endpoint.');
                }

                expect(scope.isDone()).toBeTrue();
            });
        });
    });

    describe('slicer', () => {
        describe('will behave like elasticsearch date reader', () => {
            const start = moment('2012-12-12T00:00:00.000Z');
            const end = moment(start.toISOString()).add(1, 'minute');
            let harness: SlicerTestHarness;

            async function makeSlicerTest(
                config: any, numOfSlicers = 1,
                recoveryData?: SlicerRecoveryData[],
                eventHook?: EventHook
            ) {
                const job = newTestJobConfig({
                    analytics: true,
                    slicers: numOfSlicers,
                    operations: [
                        config,
                        {
                            _op: 'noop'
                        }
                    ]
                });
                const myTest = new SlicerTestHarness(job, { assetDir, clients });
                if (eventHook) myTest.events.on(eventHook.event, eventHook.fn);
                await myTest.initialize(recoveryData);
                return myTest;
            }

            const opConfig = {
                _op: 'spaces_reader',
                date_field_name: 'created',
                time_resolution: 's',
                size: 100,
                index: testIndex,
                interval: 'auto',
                start: start.toISOString(),
                end: end.toISOString(),
                endpoint: baseUri,
                token: 'test-token',
            };
            async function getMeta(test: SlicerTestHarness) {
                return test.context.apis.executionContext.getMetadata('spaces_reader');
            }

            async function waitForUpdate(config: any) {
                return makeSlicerTest(config, 1, []);
            }

            beforeEach(async () => {
                scope.get(new RegExp(testIndex))
                    .reply(200, {
                        results: [{ created: start.toISOString() }],
                        total: 1
                    });

                scope.get(new RegExp(testIndex))
                    .query(true)
                    .reply(200, {
                        results: [{ created: end.toISOString() }],
                        total: 1
                    });

                scope.get(new RegExp(testIndex))
                    .query(true)
                    .reply(200, {
                        results: [{ created: end.toISOString() }],
                        total: 1
                    });
            });

            afterEach(async () => {
                if (harness) await harness.shutdown();
            });

            it('will convert auto to proper interval and update the opConfig', async () => {
                harness = await waitForUpdate(opConfig);
                const updatedConfig = await getMeta(harness);
                expect(updatedConfig.interval).toEqual([60, 's']);
            });
        });

        describe('when testing the Slicer', () => {
            const start = moment('2012-12-12T00:00:00.000Z');
            const end = moment(start.toISOString()).add(1, 'minute');
            const harness = new SlicerTestHarness(newTestJobConfig({
                name: 'simple-api-reader-job',
                lifecycle: 'once',
                max_retries: 0,
                operations: [
                    {
                        _op: 'spaces_reader',
                        query: 'slicer:query',
                        index: testIndex,
                        endpoint: baseUri,
                        token: 'test-token',
                        size: 2,
                        interval: '1m',
                        start: start.toISOString(),
                        end: end.toISOString(),
                        delay: '0s',
                        date_field_name: 'created',
                        timeout: 50
                    },
                    {
                        _op: 'noop'
                    }
                ]
            }), {});

            beforeEach(async () => {
                const query = {
                    token: 'test-token',
                    q: `created:[${start.toISOString()} TO ${end.toISOString()}} AND (slicer:query)`,
                };

                scope.get(`/${testIndex}`)
                    .query(Object.assign({ size: 1 }, query))
                    .reply(200, {
                        results: [{ created: start.toISOString() }],
                        total: 1
                    });

                scope.get(`/${testIndex}`)
                    .query(Object.assign({ size: 1 }, query))
                    .reply(200, {
                        results: [{ created: end.toISOString() }],
                        total: 1
                    });

                scope.get(`/${testIndex}`)
                    .query(Object.assign({ size: 0 }, query))
                    .reply(200, {
                        results: [],
                        total: 2
                    });

                await harness.initialize([]);
            });

            afterEach(async () => {
                await harness.shutdown();
            });

            it('should be able to generate slices', async () => {
                const slices = await harness.createSlices();

                expect(slices).toBeArrayOfSize(1);
                expect(slices[0]).toMatchObject({
                    count: 2,
                });
                expect(scope.isDone()).toBeTrue();
            });
        });
    });
});
