import 'jest-extended';
import nock from 'nock';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import moment from 'moment';
import { newTestJobConfig, SlicerRecoveryData, TestClientConfig } from '@terascope/job-components';
import { debugLogger } from '@terascope/core-utils';
import { SlicerTestHarness } from 'teraslice-test-harness';
import MockClient from '../helpers/mock_client.js';

const dirname = path.dirname(fileURLToPath(import.meta.url));

describe('spaces_reader slicer', () => {
    const baseUri = 'http://test.dev';
    const testIndex = 'details-subset';
    const assetDir = path.join(dirname, '../..');
    const maxSize = 100000;
    const token = 'test-token';
    const logger = debugLogger('test-logger');

    let clients: TestClientConfig[];
    let defaultClient: MockClient;

    interface EventHook {
        event: string;
        fn: (event?: any) => void;
    }

    beforeEach(() => {
        defaultClient = new MockClient();
        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: defaultClient,
                    logger
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

    describe('will behave like elasticsearch date reader', () => {
        const start = moment('2012-12-12T00:00:00.000Z');
        const end = moment(start.toISOString()).add(1, 'minute');
        let harness: SlicerTestHarness;

        async function makeSlicerTest(
            config: any,
            numOfSlicers?: number,
            recoveryData?: SlicerRecoveryData[],
            eventHook?: EventHook
        ) {
            const job = newTestJobConfig({
                analytics: true,
                slicers: numOfSlicers ?? 1,
                operations: [
                    config,
                    {
                        _op: 'noop'
                    }
                ]
            });
            const myTest = new SlicerTestHarness(job, { assetDir, clients });

            if (eventHook) {
                myTest.events.on(eventHook.event, eventHook.fn);
            }

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
            token,
        };

        async function getMeta(test: SlicerTestHarness) {
            return test.context.apis.executionContext.getMetadata('spaces_reader');
        }

        async function waitForUpdate(config: any) {
            return makeSlicerTest(config, 1, []);
        }

        beforeEach(async () => {
            scope.get(`/${testIndex}/_info?token=${token}`)
                .reply(200, {
                    params: {
                        size: {
                            max: maxSize
                        }
                    }
                });

            scope.post(new RegExp(testIndex))
                .reply(200, {
                    results: [{ created: start.toISOString() }],
                    total: 1
                });

            scope.post(new RegExp(testIndex))
                .reply(200, {
                    results: [{ created: end.toISOString() }],
                    total: 1
                });

            scope.post(new RegExp(testIndex))
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
            expect(updatedConfig).toMatchObject({
                0: {
                    interval: [60, 's']
                }
            });
        });
    });

    describe('when connected to a spaces server', () => {
        const start = moment('2012-12-12T00:00:00.000Z');
        const end = moment(start.toISOString()).add(1, 'minute');
        const variables = {
            '@foo': 'foo',
            $bar: 'bar'
        };

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
                    token,
                    size: 2,
                    interval: '1m',
                    start: start.toISOString(),
                    end: end.toISOString(),
                    delay: '0s',
                    date_field_name: 'created',
                    timeout: 50,
                    variables
                },
                {
                    _op: 'noop'
                }
            ]
        }), {});

        beforeEach(async () => {
            const query = {
                q: `created:[${start.toISOString()} TO ${end.toISOString()}} AND (slicer:query)`,
                size: 0,
                variables,
                track_total_hits: 3
            };

            scope.get(`/${testIndex}/_info?token=${token}`)
                .reply(200, {
                    params: {
                        size: {
                            max: maxSize
                        }
                    }
                });

            scope.post(`/${testIndex}?token=${token}`, query)
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

/*
{
      {
        q: 'created:[2012-12-12T00:00:00.000Z TO 2012-12-12T00:01:00.000Z} AND (slicer:query)',
        size: 0,
        variables: { '@foo': 'foo', '$bar': 'bar' },
        track_total_hits: true
      },
      start: '2012-12-12T00:00:00.000Z',
      end: '2012-12-12T00:01:00.000Z'
    }
*/
