import 'jest-extended';
import {
    pDelay, LifeCycle, SlicerRecoveryData,
    AnyObject, sortBy, SliceRequest, debugLogger,
    TestClientConfig
} from '@terascope/job-components';
import { ElasticsearchTestHelpers } from 'elasticsearch-store';
import moment from 'moment';
import { SlicerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { IDType } from '@terascope/elasticsearch-asset-apis';
import {
    TEST_INDEX_PREFIX,
    makeClient,
    cleanupIndex,
    populateIndex,
} from '../helpers/index.js';

describe('elasticsearch_reader slicer', () => {
    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_slicer_`;
    const logger = debugLogger('test-logger');

    function makeIndex(str: string) {
        return `${readerIndex}_${str}`;
    }
    const evenSpread = ElasticsearchTestHelpers.EvenDateData;
    const unevenSpread = ElasticsearchTestHelpers.UnevenDateData;

    const evenIndex = makeIndex('even_data');
    const unevenIndex = makeIndex('uneven_data');
    const docType = '_doc';

    const evenOriginalStart = '2019-04-26T15:00:23.201Z';
    const evenOriginalEnd = '2019-04-26T15:00:23.394Z';

    async function consume(test: SlicerTestHarness): Promise<SliceRequest[]> {
        const results: SliceRequest[] = [];

        async function recurse(): Promise<void> {
            const slices = await test.createSlices();
            const data = slices.filter(Boolean) as SliceRequest[];

            if (data.length > 0) {
                results.push(...data);
                return recurse();
            }
        }

        await recurse();

        return results;
    }

    const evenBulkData = evenSpread.data;
    const unevenBulkData = unevenSpread.data;

    let harness: SlicerTestHarness;
    let esClient: any;
    let clients: TestClientConfig[];

    beforeAll(async () => {
        esClient = await makeClient();

        clients = [
            {
                type: 'elasticsearch-next',
                endpoint: 'default',
                createClient: async () => ({
                    client: esClient,
                    logger
                })
            }
        ];

        await cleanupIndex(esClient, makeIndex('*'));

        await Promise.all([
            await populateIndex(
                esClient, evenIndex, evenSpread.EvenDataType, evenBulkData, docType
            ),
            await populateIndex(
                esClient, unevenIndex, unevenSpread.UnevenDataTypeFields, unevenBulkData, docType
            )
        ]);
    });

    afterAll(async () => {
        await cleanupIndex(esClient, makeIndex('*'));
    });

    afterEach(async () => {
        if (harness) {
            harness.events.emit('worker:shutdown');
            await harness.shutdown();
        }
    });

    async function getMeta(test: SlicerTestHarness) {
        return test.context.apis.executionContext.getMetadata('elasticsearch_reader');
    }

    interface EventHook {
        event: string;
        fn: (event?: any) => void;
    }

    interface SlicerTestArgs {
        opConfig: AnyObject | undefined;
        numOfSlicers?: number;
        recoveryData?: SlicerRecoveryData[];
        eventHook?: EventHook;
        lifecycle?: LifeCycle;
    }

    const defaults = {
        _op: 'elasticsearch_reader',
        time_resolution: 'ms',
        date_field_name: 'created',
        size: 50,
        index: evenIndex,
        type: docType
    };

    async function makeSlicerTest({
        opConfig = {},
        numOfSlicers = 1,
        recoveryData,
        eventHook,
        lifecycle = 'once'
    }: SlicerTestArgs) {
        const config = Object.assign({}, defaults, opConfig);

        const job = newTestJobConfig({
            analytics: true,
            slicers: numOfSlicers,
            lifecycle,
            operations: [
                config,
                {
                    _op: 'noop'
                }
            ],
        });

        harness = new SlicerTestHarness(job, { clients });
        if (eventHook) harness.events.on(eventHook.event, eventHook.fn);

        await harness.initialize(recoveryData);

        return harness;
    }

    it('can create a slicer', async () => {
        const opConfig = {
            time_resolution: 's',
            size: 50,
        };

        const test = await makeSlicerTest({ opConfig });
        const slicer = test.slicer();

        expect(slicer.slicers()).toEqual(1);
    });

    it('can create multiple slicers', async () => {
        const opConfig = {};
        const numOfSlicers = 2;
        const test = await makeSlicerTest({ opConfig, numOfSlicers });
        const slicer = test.slicer();

        expect(slicer.slicers()).toEqual(2);
    });

    it('slicers will throw if date_field_name does not exist on docs in the index', async () => {
        const opConfig = { date_field_name: 'date' };

        await expect(makeSlicerTest({ opConfig })).toReject();
    });

    describe('when start and end parameters and generate updates for range of job', () => {
        it('can handle no start or end (auto)', async () => {
            const test = await makeSlicerTest({ opConfig: {} });
            const update = await getMeta(test);

            expect(update).toEqual({
                0: {
                    start: evenOriginalStart,
                    end: evenOriginalEnd,
                    interval: [9, 'ms'],
                    count: 1000
                }
            });

            const [slice] = await test.createSlices();

            expect(slice).toEqual({
                start: evenOriginalStart,
                end: '2019-04-26T15:00:23.210Z',
                limit: evenOriginalEnd,
                count: 14,
                holes: [],
            });
        });

        it('can handle only start specified', async () => {
            const start = '2019-04-26T15:00:23.250Z';
            const test = await makeSlicerTest({ opConfig: { start } });
            const update = await getMeta(test);

            expect(update).toEqual({
                0: {
                    start,
                    end: evenOriginalEnd,
                    interval: [8, 'ms'],
                    count: 868
                }
            });

            const [slice] = await test.createSlices();

            expect(slice).toEqual({
                start,
                end: '2019-04-26T15:00:23.258Z',
                limit: evenOriginalEnd,
                count: 48,
                holes: []
            });
        });

        it('can handle only end specified', async () => {
            const end = '2019-04-26T15:00:23.280Z';
            const test = await makeSlicerTest({ opConfig: { end } });
            const update = await getMeta(test);

            expect(update).toEqual({
                0: {
                    start: evenOriginalStart,
                    end,
                    interval: [13, 'ms'],
                    count: 275
                }
            });

            const [slice] = await test.createSlices();

            expect(slice).toEqual({
                start: evenOriginalStart,
                end: '2019-04-26T15:00:23.214Z',
                limit: end,
                count: 25,
                holes: []
            });
        });
    });

    it('slicer will not error out if query returns no results', async () => {
        const opConfig = {
            query: 'some:luceneQueryWithNoResults'
        };
        const test = await makeSlicerTest({ opConfig });
        const results = await test.createSlices();

        expect(results).toEqual([null]);
    });

    it('slicer can produce date slices', async () => {
        const opConfig = {
            time_resolution: 'ms',
            size: 200
        };

        const test = await makeSlicerTest({ opConfig });
        const getAllSlices = await test.getAllSlices();

        const expectedResults = [
            {
                start: '2019-04-26T15:00:23.201Z',
                end: '2019-04-26T15:00:23.239Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 99
            },
            {
                start: '2019-04-26T15:00:23.239Z',
                end: '2019-04-26T15:00:23.277Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 169
            },
            {
                start: '2019-04-26T15:00:23.277Z',
                end: '2019-04-26T15:00:23.315Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 172
            },
            {
                start: '2019-04-26T15:00:23.315Z',
                end: '2019-04-26T15:00:23.334Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 148
            },
            {
                start: '2019-04-26T15:00:23.334Z',
                end: '2019-04-26T15:00:23.372Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 199
            },
            {
                start: '2019-04-26T15:00:23.372Z',
                end: '2019-04-26T15:00:23.383Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 111
            },
            {
                start: '2019-04-26T15:00:23.383Z',
                end: '2019-04-26T15:00:23.394Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 102
            }
        ];

        // this signals the end of slices
        expect(getAllSlices.pop()).toBeNull();

        getAllSlices.forEach((slice, index) => {
            expect(slice).toMatchObject(expectedResults[index]);
        });
    });

    it('can run a persistent reader', async () => {
        const delay: [number, moment.unitOfTime.Base] = [100, 'ms'];
        const proximateBeforeStartTime = new Date();
        const proximateBeforeDelayedBoundary = moment.utc(proximateBeforeStartTime)
            .subtract(delay[0], delay[1]);

        const opConfig = {
            size: 100,
            interval: '100ms',
            delay: delay.join(''),

        };

        function isInBetween(val: string, firstDate: any, secondDate: any) {
            return moment(val).isBetween(firstDate, secondDate);
        }

        const test = await makeSlicerTest({ opConfig, lifecycle: 'persistent' });

        const firstSegment = await consume(test);
        const secondSegment = await consume(test);

        const proximateAfterStartTime = new Date();
        const proximateAfterDelayedBoundary = moment.utc(proximateAfterStartTime)
            .subtract(delay[0], delay[1]);

        const firstSegmentStartSlice = firstSegment[0];
        const firstSegmentLastSlice = firstSegment[firstSegment.length - 1];

        expect(
            isInBetween(
                firstSegmentLastSlice.end,
                proximateBeforeDelayedBoundary,
                proximateAfterDelayedBoundary
            )
        ).toBeTrue();

        expect(
            moment(firstSegmentStartSlice.limit).diff(firstSegmentStartSlice.start)
        ).toEqual(delay[0]);

        expect(
            proximateAfterDelayedBoundary.diff(firstSegmentLastSlice.limit) <= delay[0]
        ).toBeTrue();

        // we are filtering out nulls
        expect(secondSegment).toBeArrayOfSize(0);

        await pDelay(110);

        const thirdSegment = await consume(test);

        expect(thirdSegment.length >= 1).toBeTrue();

        expect(thirdSegment[0].start).toEqual(firstSegmentLastSlice.limit);
    });

    it('slicer can reduce date slices down to size', async () => {
        const opConfig = { size: 50 };
        let hasRecursed = false;

        function hasRecursedEvent() {
            hasRecursed = true;
        }

        const eventHook = { event: 'slicer:slice:recursion', fn: hasRecursedEvent };
        const test = await makeSlicerTest({
            opConfig,
            eventHook
        });

        const allSlices = await test.getAllSlices();
        const expectedResults = [
            {
                start: '2019-04-26T15:00:23.201Z',
                end: '2019-04-26T15:00:23.210Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 14
            },
            {
                start: '2019-04-26T15:00:23.210Z',
                end: '2019-04-26T15:00:23.219Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 30
            },
            {
                start: '2019-04-26T15:00:23.219Z',
                end: '2019-04-26T15:00:23.228Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 27
            },
            {
                start: '2019-04-26T15:00:23.228Z',
                end: '2019-04-26T15:00:23.237Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 25
            },
            {
                start: '2019-04-26T15:00:23.237Z',
                end: '2019-04-26T15:00:23.246Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 20
            },
            {
                start: '2019-04-26T15:00:23.246Z',
                end: '2019-04-26T15:00:23.255Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 44
            },
            {
                start: '2019-04-26T15:00:23.255Z',
                end: '2019-04-26T15:00:23.259Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 27
            },
            {
                start: '2019-04-26T15:00:23.259Z',
                end: '2019-04-26T15:00:23.263Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 30
            },
            {
                start: '2019-04-26T15:00:23.263Z',
                end: '2019-04-26T15:00:23.272Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 33
            },
            {
                start: '2019-04-26T15:00:23.272Z',
                end: '2019-04-26T15:00:23.281Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 28
            },
            {
                start: '2019-04-26T15:00:23.281Z',
                end: '2019-04-26T15:00:23.290Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 20
            },
            {
                start: '2019-04-26T15:00:23.290Z',
                end: '2019-04-26T15:00:23.299Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 37
            },
            {
                start: '2019-04-26T15:00:23.299Z',
                end: '2019-04-26T15:00:23.308Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 47
            },
            {
                start: '2019-04-26T15:00:23.308Z',
                end: '2019-04-26T15:00:23.312Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 30
            },
            {
                start: '2019-04-26T15:00:23.312Z',
                end: '2019-04-26T15:00:23.316Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 35
            },
            {
                start: '2019-04-26T15:00:23.316Z',
                end: '2019-04-26T15:00:23.320Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 27
            },
            {
                start: '2019-04-26T15:00:23.320Z',
                end: '2019-04-26T15:00:23.324Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 35
            },
            {
                start: '2019-04-26T15:00:23.324Z',
                end: '2019-04-26T15:00:23.328Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 28
            },
            {
                start: '2019-04-26T15:00:23.328Z',
                end: '2019-04-26T15:00:23.332Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 30
            },
            {
                start: '2019-04-26T15:00:23.332Z',
                end: '2019-04-26T15:00:23.336Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 30
            },
            {
                start: '2019-04-26T15:00:23.336Z',
                end: '2019-04-26T15:00:23.340Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 22
            },
            {
                start: '2019-04-26T15:00:23.340Z',
                end: '2019-04-26T15:00:23.344Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 29
            },
            {
                start: '2019-04-26T15:00:23.344Z',
                end: '2019-04-26T15:00:23.353Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 34
            },
            {
                start: '2019-04-26T15:00:23.353Z',
                end: '2019-04-26T15:00:23.362Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 25
            },
            {
                start: '2019-04-26T15:00:23.362Z',
                end: '2019-04-26T15:00:23.366Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 17
            },
            {
                start: '2019-04-26T15:00:23.366Z',
                end: '2019-04-26T15:00:23.370Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 40
            },
            {
                start: '2019-04-26T15:00:23.370Z',
                end: '2019-04-26T15:00:23.374Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 48
            },
            {
                start: '2019-04-26T15:00:23.374Z',
                end: '2019-04-26T15:00:23.378Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 37
            },
            {
                start: '2019-04-26T15:00:23.378Z',
                end: '2019-04-26T15:00:23.382Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 39
            },
            {
                start: '2019-04-26T15:00:23.382Z',
                end: '2019-04-26T15:00:23.386Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 34
            },
            {
                start: '2019-04-26T15:00:23.386Z',
                end: '2019-04-26T15:00:23.390Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 38
            },
            {
                start: '2019-04-26T15:00:23.390Z',
                end: '2019-04-26T15:00:23.394Z',
                limit: '2019-04-26T15:00:23.394Z',
                holes: [],
                count: 40
            }
        ];

        expect(hasRecursed).toBeTrue();

        // this signals the end of slices
        expect(allSlices.pop()).toBeNull();

        allSlices.forEach((slice, index) => {
            expect(slice).toMatchObject(expectedResults[index]);
        });
    });

    it('slicer can do a simple expansion of date slices up to find data', async () => {
        // stopping before big slice
        const end = '2020-08-12T16:00:00.000Z';
        const opConfig = {
            time_resolution: 's',
            size: 100,
            index: unevenIndex,
            interval: '1m',
            end
        };

        let hasExpanded = false;

        function hasExpandedFn() {
            hasExpanded = true;
        }

        const eventHook = { event: 'slicer:slice:range_expansion', fn: hasExpandedFn };

        const test = await makeSlicerTest({
            opConfig,
            eventHook
        });

        const allSlices = await test.getAllSlices();

        const expectedResults = [
            {
                start: '2020-08-12T15:41:01.000Z',
                end: '2020-08-12T15:42:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T15:42:01.000Z',
                end: '2020-08-12T15:43:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T15:43:01.000Z',
                end: '2020-08-12T15:44:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 5
            },
            {
                start: '2020-08-12T15:44:01.000Z',
                end: '2020-08-12T15:45:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 18
            },
            {
                start: '2020-08-12T15:45:01.000Z',
                end: '2020-08-12T15:46:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T15:46:01.000Z',
                end: '2020-08-12T15:47:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T15:47:01.000Z',
                end: '2020-08-12T15:48:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 6
            },
            {
                start: '2020-08-12T15:48:01.000Z',
                end: '2020-08-12T15:49:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T15:49:01.000Z',
                end: '2020-08-12T15:50:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T15:50:01.000Z',
                end: '2020-08-12T15:51:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 7
            },
            {
                start: '2020-08-12T15:51:01.000Z',
                end: '2020-08-12T15:55:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 4
            },
            {
                start: '2020-08-12T15:55:01.000Z',
                end: '2020-08-12T15:56:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 20
            },
            {
                start: '2020-08-12T15:56:01.000Z',
                end: '2020-08-12T15:57:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 16
            },
            {
                start: '2020-08-12T15:57:01.000Z',
                end: '2020-08-12T15:58:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 23
            },
            {
                start: '2020-08-12T15:58:01.000Z',
                end: '2020-08-12T15:59:01.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 18
            },
            {
                start: '2020-08-12T15:59:01.000Z',
                end: '2020-08-12T16:00:00.000Z',
                limit: '2020-08-12T16:00:00.000Z',
                holes: [],
                count: 19
            }
        ];

        expect(hasExpanded).toBeTrue();

        // this signals the end of slices
        expect(allSlices.pop()).toBeNull();

        allSlices.forEach((slice, index) => {
            expect(slice).toMatchObject(expectedResults[index]);
        });
    });

    it('slicer can do an expansion of date slices up to find data even when none is returned', async () => {
        // stopping at first gap
        const end = '2020-08-12T15:52:48.000Z';
        const opConfig = {
            time_resolution: 's',
            size: 100,
            index: unevenIndex,
            interval: '1m',
            end
        };

        let hasExpanded = false;

        function hasExpandedFn() {
            hasExpanded = true;
        }

        const eventHook = { event: 'slicer:slice:range_expansion', fn: hasExpandedFn };

        const test = await makeSlicerTest({
            opConfig,
            eventHook
        });

        const allSlices = await test.getAllSlices();

        const expectedResults = [
            {
                start: '2020-08-12T15:41:01.000Z',
                end: '2020-08-12T15:42:01.000Z',
                limit: '2020-08-12T15:52:48.000Z',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T15:42:01.000Z',
                end: '2020-08-12T15:43:01.000Z',
                limit: '2020-08-12T15:52:48.000Z',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T15:43:01.000Z',
                end: '2020-08-12T15:44:01.000Z',
                limit: '2020-08-12T15:52:48.000Z',
                holes: [],
                count: 5
            },
            {
                start: '2020-08-12T15:44:01.000Z',
                end: '2020-08-12T15:45:01.000Z',
                limit: '2020-08-12T15:52:48.000Z',
                holes: [],
                count: 18
            },
            {
                start: '2020-08-12T15:45:01.000Z',
                end: '2020-08-12T15:46:01.000Z',
                limit: '2020-08-12T15:52:48.000Z',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T15:46:01.000Z',
                end: '2020-08-12T15:47:01.000Z',
                limit: '2020-08-12T15:52:48.000Z',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T15:47:01.000Z',
                end: '2020-08-12T15:48:01.000Z',
                limit: '2020-08-12T15:52:48.000Z',
                holes: [],
                count: 6
            },
            {
                start: '2020-08-12T15:48:01.000Z',
                end: '2020-08-12T15:49:01.000Z',
                limit: '2020-08-12T15:52:48.000Z',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T15:49:01.000Z',
                end: '2020-08-12T15:50:01.000Z',
                limit: '2020-08-12T15:52:48.000Z',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T15:50:01.000Z',
                end: '2020-08-12T15:51:01.000Z',
                limit: '2020-08-12T15:52:48.000Z',
                holes: [],
                count: 7
            },
            {
                start: '2020-08-12T15:51:01.000Z',
                end: '2020-08-12T15:52:48.000Z',
                limit: '2020-08-12T15:52:48.000Z',
                holes: [],
                count: 0
            }
        ];

        expect(hasExpanded).toBeTrue();

        // this signals the end of slices
        expect(allSlices.pop()).toBeNull();

        allSlices.forEach((slice, index) => {
            expect(slice).toMatchObject(expectedResults[index]);
        });

        const lastSlice = expectedResults[expectedResults.length - 1];
        expect(lastSlice.end).toEqual(end);
    });

    it('slicer can do expansion of date slices with large slices', async () => {
        const opConfig = {
            time_resolution: 's',
            size: 50,
            index: unevenIndex,
            interval: '1m',
        };

        let hasExpanded = false;
        function hasExpandedFn() {
            hasExpanded = true;
        }

        const eventHook = { event: 'slicer:slice:range_expansion', fn: hasExpandedFn };
        const test = await makeSlicerTest({ opConfig, eventHook });

        const allSlices = await test.getAllSlices();

        const expectedResults = [
            {
                start: '2020-08-12T15:41:01.000Z',
                end: '2020-08-12T15:42:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T15:42:01.000Z',
                end: '2020-08-12T15:43:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T15:43:01.000Z',
                end: '2020-08-12T15:44:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 5
            },
            {
                start: '2020-08-12T15:44:01.000Z',
                end: '2020-08-12T15:45:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 18
            },
            {
                start: '2020-08-12T15:45:01.000Z',
                end: '2020-08-12T15:46:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T15:46:01.000Z',
                end: '2020-08-12T15:47:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T15:47:01.000Z',
                end: '2020-08-12T15:48:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 6
            },
            {
                start: '2020-08-12T15:48:01.000Z',
                end: '2020-08-12T15:49:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T15:49:01.000Z',
                end: '2020-08-12T15:50:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T15:50:01.000Z',
                end: '2020-08-12T15:51:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 7
            },
            {
                start: '2020-08-12T15:51:01.000Z',
                end: '2020-08-12T15:55:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 4
            },
            {
                start: '2020-08-12T15:55:01.000Z',
                end: '2020-08-12T15:56:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 20
            },
            {
                start: '2020-08-12T15:56:01.000Z',
                end: '2020-08-12T15:57:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 16
            },
            {
                start: '2020-08-12T15:57:01.000Z',
                end: '2020-08-12T15:58:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 23
            },
            {
                start: '2020-08-12T15:58:01.000Z',
                end: '2020-08-12T15:59:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 18
            },
            {
                start: '2020-08-12T15:59:01.000Z',
                end: '2020-08-12T16:00:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 19
            },
            {
                start: '2020-08-12T16:00:01.000Z',
                end: '2020-08-12T16:04:30.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 0
            },
            {
                start: '2020-08-12T16:04:30.000Z',
                end: '2020-08-12T16:04:52.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 0
            },
            {
                start: '2020-08-12T16:04:52.000Z',
                end: '2020-08-12T16:04:58.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 0
            },
            {
                start: '2020-08-12T16:04:58.000Z',
                end: '2020-08-12T16:04:59.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 0
            },
            {
                start: '2020-08-12T16:04:59.000Z',
                end: '2020-08-12T16:05:01.000Z',
                limit: '2020-08-12T16:05:01.000Z',
                holes: [],
                count: 100
            }
        ];

        expect(hasExpanded).toBeTrue();

        // this signals the end of slices
        expect(allSlices.pop()).toBeNull();

        allSlices.forEach((slice, index) => {
            expect(slice).toMatchObject(expectedResults[index]);
        });
    });

    it('slicer can will recurse down to smallest factor using "s" format', async () => {
        const opConfig = {
            time_resolution: 's',
            size: 10,
            index: unevenIndex,
            interval: '5m',
            start: '2020-08-12T16:04:00.000Z',
            end: '2020-08-12T16:06:00.000Z'
        };

        const expectedSlice = {
            start: '2020-08-12T16:04:00.000Z',
            end: '2020-08-12T16:05:01.000Z',
            limit: '2020-08-12T16:06:00.000Z',
            holes: [],
            count: 100
        };

        const test = await makeSlicerTest({ opConfig });
        const [resultsS] = await test.createSlices();

        expect(resultsS).toMatchObject(expectedSlice);
    });

    it('slicer can will recurse down to smallest factor using "ms" format', async () => {
        const opConfig = {
            time_resolution: 'ms',
            size: 10,
            index: unevenIndex,
            interval: '5m',
            start: '2020-08-12T16:04:00.000Z',
            end: '2020-08-12T16:06:00.000Z'
        };

        const expectedSlice = {
            start: '2020-08-12T16:04:00.000Z',
            end: '2020-08-12T16:05:00.001Z',
            limit: '2020-08-12T16:06:00.000Z',
            holes: [],
            count: 100
        };

        // Need to run them separately so they get a different client
        const test = await makeSlicerTest({ opConfig });
        const [resultsMS] = await test.createSlices();

        expect(resultsMS).toMatchObject(expectedSlice);
    });

    it('slicer can will recurse down to smallest factor and subslice by key', async () => {
        const opConfig = {
            time_resolution: 's',
            size: 10,
            index: unevenIndex,
            interval: '5m',
            subslice_by_key: true,
            subslice_key_threshold: 50,
            key_type: IDType.hexadecimal,
            field: 'uuid',
            type: docType,
            start: '2020-08-12T16:05:00Z'
        };

        const test = await makeSlicerTest({ opConfig });
        const allSlices = await test.getAllSlices();

        const dates = {
            start: '2020-08-12T16:05:00.000Z',
            end: '2020-08-12T16:05:01.000Z',
            limit: '2020-08-12T16:05:01.000Z'
        };

        const expectedKeyCounts = [
            {
                keys: ['0'],
                count: 9,
            },
            {
                keys: ['1'],
                count: 5,
            },
            {
                keys: ['2'],
                count: 4,
            },
            {
                keys: ['3'],
                count: 8,
            },
            {
                keys: ['4'],
                count: 9,
            },
            {
                keys: ['5'],
                count: 6,
            },
            {
                keys: ['6'],
                count: 7,
            },
            {
                keys: ['7'],
                count: 9,
            },
            {
                keys: ['8'],
                count: 7,
            },
            {
                keys: ['9'],
                count: 5,
            },
            {
                keys: ['a'],
                count: 6,
            },
            {
                keys: ['b'],
                count: 6,
            },
            {
                keys: ['c'],
                count: 2,
            },
            {
                keys: ['d'],
                count: 7,
            },
            {
                keys: ['e'],
                count: 4,
            },
            {
                keys: ['f'],
                count: 6,
            },
        ];

        const expectedResults = expectedKeyCounts
            .map((obj) => ({ ...obj, ...dates }));

        // this signals the end of slices
        expect(allSlices.pop()).toBeNull();

        allSlices.forEach((slice, index) => {
            expect(slice).toMatchObject(expectedResults[index]);
        });
    });

    it('slicer can enter recovery and return to the last slice state', async () => {
        const opConfig = {
            time_resolution: 'ms',
            size: 200
        };

        const expectedNextSlice = {
            start: '2019-04-26T15:00:23.334Z',
            end: '2019-04-26T15:00:23.372Z',
            limit: '2019-04-26T15:00:23.394Z',
            holes: [],
            count: 199
        };

        const recoveryData = [
            {
                lastSlice: {
                    start: '2019-04-26T15:00:23.315Z',
                    end: '2019-04-26T15:00:23.334Z',
                    limit: '2019-04-26T15:00:23.394Z',
                    holes: [],
                    count: 148
                },
                slicer_id: 0
            }
        ];

        const test = await makeSlicerTest({ opConfig, recoveryData });

        const [results] = await test.createSlices();
        expect(results).toMatchObject(expectedNextSlice);
    });

    it('multiple slicers can enter recovery and return to the last slice state', async () => {
        const opConfig = {
            time_resolution: 'ms',
            size: 200,
            index: unevenIndex
        };

        const recoveryData = [
            {
                lastSlice: {
                    start: '2020-08-12T15:40:48.470Z',
                    end: '2020-08-12T15:50:46.470Z',
                    limit: '2020-08-12T15:50:48.470Z',
                    holes: [],
                    count: 148
                },
                slicer_id: 0
            },
            {
                lastSlice: {
                    start: '2020-08-12T16:04:00.000Z',
                    end: '2020-08-12T16:04:59.000Z',
                    limit: '2020-08-12T16:05:00.000Z',
                    holes: [],
                    count: 111
                },
                slicer_id: 1
            }
        ];

        const numOfSlicers = 2;

        const test = await makeSlicerTest({ opConfig, numOfSlicers, recoveryData });

        const allSlices = await test.getAllSlices();
        const slices = allSlices.filter(Boolean);

        expect(slices).toBeArrayOfSize(2);
    });

    it('slicer can enter recovery and return to the last slice state when number of slicers have increased (1 => 2, even increase)', async () => {
        const opConfig = {
            time_resolution: 'ms',
            size: 200
        };

        const recoveryData = [
            {
                lastSlice: {
                    start: '2019-04-26T15:00:23.315Z',
                    end: '2019-04-26T15:00:23.334Z',
                    limit: '2019-04-26T15:00:23.394Z',
                    holes: [],
                    count: 148
                },
                slicer_id: 0
            }
        ];

        const test = await makeSlicerTest({ opConfig, recoveryData, numOfSlicers: 2 });

        const allSlices = await test.getAllSlices();

        const [firstSlice, secondSlice, thirdSlice] = sortBy(allSlices.filter(Boolean), 'start') as AnyObject[];

        expect(firstSlice.limit).toEqual(secondSlice.start);

        expect(secondSlice.limit).toEqual(thirdSlice.limit);
    });

    it('slicer can enter recovery and return to the last slice state when number of slicers have increased (3 => 5, odd increase)', async () => {
        const opConfig = {
            time_resolution: 's',
            size: 100,
            index: unevenIndex,
            interval: '5m',
        };

        const recoveryData = [
            {
                lastSlice: {
                    start: '2020-08-12T15:41:01.000Z',
                    end: '2020-08-12T15:46:01.000Z',
                    limit: '2020-08-12T15:49:01.000Z',
                    holes: [],
                    count: 58
                },
                slicer_id: 0
            },
            {
                lastSlice: {
                    start: '2020-08-12T15:49:01.000Z',
                    end: '2020-08-12T15:54:01.000Z',
                    limit: '2020-08-12T15:57:01.000Z',
                    holes: [],
                    count: 16
                },
                slicer_id: 1
            },
            {
                lastSlice: {
                    start: '2020-08-12T15:57:01.000Z',
                    end: '2020-08-12T16:02:01.000Z',
                    limit: '2020-08-12T16:05:01.000Z',
                    holes: [],
                    count: 60
                },
                slicer_id: 2
            }
        ];

        const test = await makeSlicerTest({ opConfig, recoveryData, numOfSlicers: 5 });

        const allSlices = await test.getAllSlices({ fullResponse: true });

        const [
            slice1, slice2, slice3, slice4, slice5
        ] = sortBy(allSlices.filter(Boolean), 'slicer_id') as AnyObject[];

        // the first and second slicers break up the first segment
        expect(slice1.request.start).toEqual(recoveryData[0].lastSlice.end);
        expect(slice2.request.limit).toEqual(recoveryData[0].lastSlice.limit);

        // the third and fourth slicers break up the second segment
        expect(slice3.request.start).toEqual(recoveryData[1].lastSlice.end);
        expect(slice4.request.limit).toEqual(recoveryData[1].lastSlice.limit);

        // the fifth slicers break up the last segment
        expect(slice5.request.start).toEqual(recoveryData[2].lastSlice.end);
        expect(slice5.request.limit).toEqual(recoveryData[2].lastSlice.limit);
    });

    it('slicer can enter recovery and return to the last slice state when number of slicers have decreased (2 => 1, even increase)', async () => {
        const opConfig = {
            time_resolution: 's',
            size: 100,
            index: unevenIndex,
            interval: '5m',
        };
        // 58, 63 => 121
        const recoveryData = [
            {
                lastSlice: {
                    start: '2020-08-12T15:41:01.000Z',
                    end: '2020-08-12T15:46:01.000Z',
                    limit: '2020-08-12T15:53:01.000Z',
                    holes: [],
                    count: 58
                },
                slicer_id: 0
            },
            {
                lastSlice: {
                    start: '2020-08-12T15:53:01.000Z',
                    end: '2020-08-12T15:58:01.000Z',
                    limit: '2020-08-12T16:05:01.000Z',
                    holes: [],
                    count: 63
                },
                slicer_id: 1
            }
        ];

        const test = await makeSlicerTest({ opConfig, recoveryData, numOfSlicers: 1 });

        const allSlices = await test.getAllSlices({ fullResponse: true });
        const [
            slice1, slice2, slice3, slice4
        ] = sortBy(allSlices.filter(Boolean), 'slicer_id') as AnyObject[];

        // the first and second slicers break up the first segment
        expect(slice1.request.start).toEqual(recoveryData[0].lastSlice.end);
        expect(slice2.request.end).toEqual(recoveryData[1].lastSlice.start);

        // the third and fourth slicers break up the second segment
        expect(slice3.request.start).toEqual(recoveryData[1].lastSlice.end);
        expect(slice4.request.limit).toEqual(recoveryData[1].lastSlice.limit);
    });
});
