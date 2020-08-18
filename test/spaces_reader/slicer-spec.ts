import 'jest-extended';
import nock from 'nock';
import path from 'path';
import moment from 'moment';
import { newTestJobConfig, SlicerRecoveryData } from '@terascope/job-components';
import { SlicerTestHarness } from 'teraslice-test-harness';
import MockClient from '../helpers/mock_client';

describe('spaces_reader slicer', () => {
    const baseUri = 'http://test.dev';
    const testIndex = 'details-subset';
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

    describe('when connected to a spaces server', () => {
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
