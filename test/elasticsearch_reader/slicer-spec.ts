import 'jest-extended';
import {
    DataEntity,
    pDelay,
    LifeCycle,
    SlicerRecoveryData,
    AnyObject,
    sortBy
} from '@terascope/job-components';
import moment from 'moment';
import { getESVersion } from 'elasticsearch-store';
import { SlicerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { IDType } from '../../asset/src/id_reader/interfaces';
import { dateFormatSeconds, dateFormat } from '../../asset/src/elasticsearch_reader/elasticsearch_date_range/helpers';
import {
    TEST_INDEX_PREFIX,
    ELASTICSEARCH_VERSION,
    makeClient,
    cleanupIndex,
    populateIndex,
    formatWildcardQuery
} from '../helpers';
import evenSpread from '../fixtures/data/even-spread';
import unevenSpread from '../fixtures/data/uneven-spread';

describe('elasticsearch_reader slicer', () => {
    const esClient = makeClient();
    const readerIndex = `${TEST_INDEX_PREFIX}_elasticsearch_slicer_`;

    function makeIndex(str: string) {
        return `${readerIndex}_${str}`;
    }

    const evenIndex = makeIndex(evenSpread.index);
    const unevenIndex = makeIndex(unevenSpread.index);

    const version = getESVersion(esClient);
    const docType = version === 5 ? 'events' : '_doc';

    const evenOriginalStart = formatDate('2019-04-26T08:00:23.201-07:00');
    const evenOriginalEnd = '2019-04-26T08:00:23.394-07:00';

    let harness: SlicerTestHarness;

    const clients = [
        {
            type: 'elasticsearch',
            endpoint: 'default',
            create: () => ({
                client: esClient
            }),
            config: {
                apiVersion: ELASTICSEARCH_VERSION
            }
        }
    ];

    const evenBulkData = evenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));
    const unevenBulkData = unevenSpread.data.map((obj) => DataEntity.make(obj, { _key: obj.uuid }));

    beforeAll(async () => {
        await cleanupIndex(esClient, makeIndex('*'));
        await Promise.all([
            await populateIndex(esClient, evenIndex, evenSpread.types, evenBulkData, docType),
            await populateIndex(esClient, unevenIndex, unevenSpread.types, unevenBulkData, docType)
        ]);
    });

    // afterAll(async () => {
    //     await cleanupIndex(esClient, makeIndex('*'));
    // });

    afterEach(async () => {
        if (harness) {
            harness.events.emit('worker:shutdown');
            await harness.shutdown();
        }
    });

    function makeDate(format: string) {
        return moment(moment().format(format));
    }

    function formatDate(date: string | Date | number, format = dateFormat) {
        return moment(date).format(format);
    }

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

    describe('it can generate updates for changes', () => {
        it('with no start or end (auto)', async () => {
            const test = await makeSlicerTest({ opConfig: {} });
            const update = await getMeta(test);
            // console.log('update', update, evenOriginalStart)
            expect(update.start).toEqual(evenOriginalStart);
            expect(update.end).toEqual(evenOriginalEnd);
            expect(update.interval).toEqual([9, 'ms']);
        });

        it('with start specified', async () => {
            const start = '2019-04-26T08:00:23.250-07:00';
            const test = await makeSlicerTest({ opConfig: { start } });
            const update = await getMeta(test);

            expect(update.start).toEqual(start);
            expect(update.end).toEqual(evenOriginalEnd);
            expect(update.interval).toEqual([8, 'ms']);
        });

        it('with end specified', async () => {
            const end = '2019-04-26T08:00:23.280-07:00';
            const test = await makeSlicerTest({ opConfig: { end } });
            const update = await getMeta(test);

            expect(update.start).toEqual(evenOriginalStart);
            expect(update.end).toEqual(end);
            expect(update.interval).toEqual([13, 'ms']);
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
                start: '2019-04-26T08:00:23.201-07:00',
                end: '2019-04-26T08:00:23.239-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 99
            },
            {
                start: '2019-04-26T08:00:23.239-07:00',
                end: '2019-04-26T08:00:23.277-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 169
            },
            {
                start: '2019-04-26T08:00:23.277-07:00',
                end: '2019-04-26T08:00:23.315-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 172
            },
            {
                start: '2019-04-26T08:00:23.315-07:00',
                end: '2019-04-26T08:00:23.334-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 148
            },
            {
                start: '2019-04-26T08:00:23.334-07:00',
                end: '2019-04-26T08:00:23.372-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 199
            },
            {
                start: '2019-04-26T08:00:23.372-07:00',
                end: '2019-04-26T08:00:23.383-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 111
            },
            {
                start: '2019-04-26T08:00:23.383-07:00',
                end: '2019-04-26T08:00:23.394-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 102
            },
        ];

        // this signals the end of slices
        expect(getAllSlices.pop()).toBeNull();

        getAllSlices.forEach((slice, index) => {
            expect(slice).toMatchObject(expectedResults[index]);
        });
    });

    it('can run a persistent reader', async () => {
        const delay: [number, moment.unitOfTime.Base] = [100, 'ms'];
        const start = evenOriginalStart;
        const delayedBoundary = moment(start).subtract(delay[0], delay[1]);

        const opConfig = {
            size: 100,
            interval: '100ms',
            delay: delay.join('')
        };

        const test = await makeSlicerTest({ opConfig, lifecycle: 'persistent' });

        const [results] = await test.createSlices();

        expect(results).toBeDefined();

        expect(results?.start).toBeDefined();
        expect(results?.end).toBeDefined();
        expect(results?.count).toBeDefined();

        const now1 = makeDate(dateFormat);
        expect(moment(results?.end).isBetween(delayedBoundary, now1)).toEqual(true);

        const [results2] = await test.createSlices();

        expect(results2).toEqual(null);

        await pDelay(110);

        const [results3] = await test.createSlices();

        expect(results3).toBeDefined();
        expect(results3?.start).toBeDefined();
        expect(results3?.end).toBeDefined();
        expect(results3?.count).toBeDefined();

        const [results4] = await test.createSlices();
        expect(results4).toEqual(null);

        const [results5] = await test.createSlices();
        expect(results5).toEqual(null);
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
                start: '2019-04-26T08:00:23.201-07:00',
                end: '2019-04-26T08:00:23.210-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 14
            },
            {
                start: '2019-04-26T08:00:23.210-07:00',
                end: '2019-04-26T08:00:23.219-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 30
            },
            {
                start: '2019-04-26T08:00:23.219-07:00',
                end: '2019-04-26T08:00:23.228-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 27
            },
            {
                start: '2019-04-26T08:00:23.228-07:00',
                end: '2019-04-26T08:00:23.237-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 25
            },
            {
                start: '2019-04-26T08:00:23.237-07:00',
                end: '2019-04-26T08:00:23.246-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 20
            },
            {
                start: '2019-04-26T08:00:23.246-07:00',
                end: '2019-04-26T08:00:23.255-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 44
            },
            {
                start: '2019-04-26T08:00:23.255-07:00',
                end: '2019-04-26T08:00:23.259-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 27
            },
            {
                start: '2019-04-26T08:00:23.259-07:00',
                end: '2019-04-26T08:00:23.263-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 30
            },
            {
                start: '2019-04-26T08:00:23.263-07:00',
                end: '2019-04-26T08:00:23.272-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 33
            },
            {
                start: '2019-04-26T08:00:23.272-07:00',
                end: '2019-04-26T08:00:23.281-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 28
            },
            {
                start: '2019-04-26T08:00:23.281-07:00',
                end: '2019-04-26T08:00:23.290-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 20
            },
            {
                start: '2019-04-26T08:00:23.290-07:00',
                end: '2019-04-26T08:00:23.299-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 37
            },
            {
                start: '2019-04-26T08:00:23.299-07:00',
                end: '2019-04-26T08:00:23.308-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 47
            },
            {
                start: '2019-04-26T08:00:23.308-07:00',
                end: '2019-04-26T08:00:23.312-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 30
            },
            {
                start: '2019-04-26T08:00:23.312-07:00',
                end: '2019-04-26T08:00:23.316-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 35
            },
            {
                start: '2019-04-26T08:00:23.316-07:00',
                end: '2019-04-26T08:00:23.320-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 27
            },
            {
                start: '2019-04-26T08:00:23.320-07:00',
                end: '2019-04-26T08:00:23.324-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 35
            },
            {
                start: '2019-04-26T08:00:23.324-07:00',
                end: '2019-04-26T08:00:23.328-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 28
            },
            {
                start: '2019-04-26T08:00:23.328-07:00',
                end: '2019-04-26T08:00:23.332-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 30
            },
            {
                start: '2019-04-26T08:00:23.332-07:00',
                end: '2019-04-26T08:00:23.336-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 30
            },
            {
                start: '2019-04-26T08:00:23.336-07:00',
                end: '2019-04-26T08:00:23.340-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 22
            },
            {
                start: '2019-04-26T08:00:23.340-07:00',
                end: '2019-04-26T08:00:23.344-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 29
            },
            {
                start: '2019-04-26T08:00:23.344-07:00',
                end: '2019-04-26T08:00:23.353-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 34
            },
            {
                start: '2019-04-26T08:00:23.353-07:00',
                end: '2019-04-26T08:00:23.362-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 25
            },
            {
                start: '2019-04-26T08:00:23.362-07:00',
                end: '2019-04-26T08:00:23.366-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 17
            },
            {
                start: '2019-04-26T08:00:23.366-07:00',
                end: '2019-04-26T08:00:23.370-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 40
            },
            {
                start: '2019-04-26T08:00:23.370-07:00',
                end: '2019-04-26T08:00:23.374-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 48
            },
            {
                start: '2019-04-26T08:00:23.374-07:00',
                end: '2019-04-26T08:00:23.378-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 37
            },
            {
                start: '2019-04-26T08:00:23.378-07:00',
                end: '2019-04-26T08:00:23.382-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 39
            },
            {
                start: '2019-04-26T08:00:23.382-07:00',
                end: '2019-04-26T08:00:23.386-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 34
            },
            {
                start: '2019-04-26T08:00:23.386-07:00',
                end: '2019-04-26T08:00:23.390-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 38
            },
            {
                start: '2019-04-26T08:00:23.390-07:00',
                end: '2019-04-26T08:00:23.394-07:00',
                limit: '2019-04-26T08:00:23.394-07:00',
                holes: [],
                count: 40
            },
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
        const end = '2020-08-12T16:00:00.000';
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
                start: '2020-08-12T08:41:01-07:00',
                end: '2020-08-12T08:42:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T08:42:01-07:00',
                end: '2020-08-12T08:43:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T08:43:01-07:00',
                end: '2020-08-12T08:44:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 5
            },
            {
                start: '2020-08-12T08:44:01-07:00',
                end: '2020-08-12T08:45:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 18
            },
            {
                start: '2020-08-12T08:45:01-07:00',
                end: '2020-08-12T08:46:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T08:46:01-07:00',
                end: '2020-08-12T08:47:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T08:47:01-07:00',
                end: '2020-08-12T08:48:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 6
            },
            {
                start: '2020-08-12T08:48:01-07:00',
                end: '2020-08-12T08:49:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T08:49:01-07:00',
                end: '2020-08-12T08:50:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T08:50:01-07:00',
                end: '2020-08-12T08:51:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 7
            },
            {
                start: '2020-08-12T08:51:01-07:00',
                end: '2020-08-12T08:55:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 4
            },
            {
                start: '2020-08-12T08:55:01-07:00',
                end: '2020-08-12T08:56:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 20
            },
            {
                start: '2020-08-12T08:56:01-07:00',
                end: '2020-08-12T08:57:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 16
            },
            {
                start: '2020-08-12T08:57:01-07:00',
                end: '2020-08-12T08:58:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 23
            },
            {
                start: '2020-08-12T08:58:01-07:00',
                end: '2020-08-12T08:59:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 18
            },
            {
                start: '2020-08-12T08:59:01-07:00',
                end: '2020-08-12T09:00:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 19
            },
            {
                start: '2020-08-12T09:00:01-07:00',
                end: '2020-08-12T09:05:01-07:00',
                limit: '2020-08-12T16:00:00-07:00',
                holes: [],
                count: 100
            },
            {
                start: '2020-08-12T09:05:01-07:00',
                end: '2020-08-12T16:00:00-07:00',
                limit: '2020-08-12T16:00:00-07:00',
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
    });

    it('slicer can do an expansion of date slices up to find data even when none is returned', async () => {
        // stopping at first gap
        const end = '2020-08-12T15:52:48.470Z';
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
                start: '2020-08-12T08:41:01-07:00',
                end: '2020-08-12T08:42:01-07:00',
                limit: '2020-08-12T08:52:48-07:00',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T08:42:01-07:00',
                end: '2020-08-12T08:43:01-07:00',
                limit: '2020-08-12T08:52:48-07:00',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T08:43:01-07:00',
                end: '2020-08-12T08:44:01-07:00',
                limit: '2020-08-12T08:52:48-07:00',
                holes: [],
                count: 5
            },
            {
                start: '2020-08-12T08:44:01-07:00',
                end: '2020-08-12T08:45:01-07:00',
                limit: '2020-08-12T08:52:48-07:00',
                holes: [],
                count: 18
            },
            {
                start: '2020-08-12T08:45:01-07:00',
                end: '2020-08-12T08:46:01-07:00',
                limit: '2020-08-12T08:52:48-07:00',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T08:46:01-07:00',
                end: '2020-08-12T08:47:01-07:00',
                limit: '2020-08-12T08:52:48-07:00',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T08:47:01-07:00',
                end: '2020-08-12T08:48:01-07:00',
                limit: '2020-08-12T08:52:48-07:00',
                holes: [],
                count: 6
            },
            {
                start: '2020-08-12T08:48:01-07:00',
                end: '2020-08-12T08:49:01-07:00',
                limit: '2020-08-12T08:52:48-07:00',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T08:49:01-07:00',
                end: '2020-08-12T08:50:01-07:00',
                limit: '2020-08-12T08:52:48-07:00',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T08:50:01-07:00',
                end: '2020-08-12T08:51:01-07:00',
                limit: '2020-08-12T08:52:48-07:00',
                holes: [],
                count: 7
            },
            {
                start: '2020-08-12T08:51:01-07:00',
                end: '2020-08-12T08:52:48-07:00',
                limit: '2020-08-12T08:52:48-07:00',
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
        expect(lastSlice.end).toEqual(formatDate(end, dateFormatSeconds));
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
                start: '2020-08-12T08:41:01-07:00',
                end: '2020-08-12T08:42:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T08:42:01-07:00',
                end: '2020-08-12T08:43:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 13
            },
            {
                start: '2020-08-12T08:43:01-07:00',
                end: '2020-08-12T08:44:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 5
            },
            {
                start: '2020-08-12T08:44:01-07:00',
                end: '2020-08-12T08:45:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 18
            },
            {
                start: '2020-08-12T08:45:01-07:00',
                end: '2020-08-12T08:46:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T08:46:01-07:00',
                end: '2020-08-12T08:47:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T08:47:01-07:00',
                end: '2020-08-12T08:48:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 6
            },
            {
                start: '2020-08-12T08:48:01-07:00',
                end: '2020-08-12T08:49:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 10
            },
            {
                start: '2020-08-12T08:49:01-07:00',
                end: '2020-08-12T08:50:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 9
            },
            {
                start: '2020-08-12T08:50:01-07:00',
                end: '2020-08-12T08:51:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 7
            },
            {
                start: '2020-08-12T08:51:01-07:00',
                end: '2020-08-12T08:55:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 4
            },
            {
                start: '2020-08-12T08:55:01-07:00',
                end: '2020-08-12T08:56:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 20
            },
            {
                start: '2020-08-12T08:56:01-07:00',
                end: '2020-08-12T08:57:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 16
            },
            {
                start: '2020-08-12T08:57:01-07:00',
                end: '2020-08-12T08:58:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 23
            },
            {
                start: '2020-08-12T08:58:01-07:00',
                end: '2020-08-12T08:59:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 18
            },
            {
                start: '2020-08-12T08:59:01-07:00',
                end: '2020-08-12T09:00:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 19
            },
            {
                start: '2020-08-12T09:00:01-07:00',
                end: '2020-08-12T09:04:30-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 0
            },
            {
                start: '2020-08-12T09:04:30-07:00',
                end: '2020-08-12T09:04:52-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 0
            },
            {
                start: '2020-08-12T09:04:52-07:00',
                end: '2020-08-12T09:04:58-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 0
            },
            {
                start: '2020-08-12T09:04:58-07:00',
                end: '2020-08-12T09:04:59-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 0
            },
            {
                start: '2020-08-12T09:04:59-07:00',
                end: '2020-08-12T09:05:01-07:00',
                limit: '2020-08-12T09:05:01-07:00',
                holes: [],
                count: 100
            },
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
            start: '2020-08-12T09:04:00-07:00',
            end: '2020-08-12T09:05:01-07:00',
            limit: '2020-08-12T09:06:00-07:00',
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
            start: '2020-08-12T09:04:00.000-07:00',
            end: '2020-08-12T09:05:00.001-07:00',
            limit: '2020-08-12T09:06:00.000-07:00',
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
            start: formatDate('2020-08-12T16:05:00.000Z', dateFormatSeconds)
        };

        const test = await makeSlicerTest({ opConfig });
        const allSlices = await test.getAllSlices();

        const dates = {
            start: '2020-08-12T09:05:00-07:00',
            end: '2020-08-12T09:05:01-07:00',
            limit: '2020-08-12T09:05:01-07:00'
        };

        const expectedKeyCounts = [
            {
                key: '0*',
                count: 9,
            },
            {
                key: '1*',
                count: 5,
            },
            {
                key: '2*',
                count: 4,
            },
            {
                key: '3*',
                count: 8,
            },
            {
                key: '4*',
                count: 9,
            },
            {
                key: '5*',
                count: 6,
            },
            {
                key: '6*',
                count: 7,
            },
            {
                key: '7*',
                count: 9,
            },
            {
                key: '8*',
                count: 7,
            },
            {
                key: '9*',
                count: 5,
            },
            {
                key: 'a*',
                count: 6,
            },
            {
                key: 'b*',
                count: 6,
            },
            {
                key: 'c*',
                count: 2,
            },
            {
                key: 'd*',
                count: 7,
            },
            {
                key: 'e*',
                count: 4,
            },
            {
                key: 'f*',
                count: 6,
            },
        ];

        const expectedResults = formatWildcardQuery(expectedKeyCounts, version, docType, 'uuid')
            .map((obj) => Object.assign({}, obj, dates));

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
            start: '2019-04-26T08:00:23.334-07:00',
            end: '2019-04-26T08:00:23.372-07:00',
            limit: '2019-04-26T08:00:23.394-07:00',
            holes: [],
            count: 199
        };

        const recoveryData = [
            {
                lastSlice: {
                    start: formatDate('2019-04-26T08:00:23.315-07:00', dateFormat),
                    end: formatDate('2019-04-26T08:00:23.334-07:00', dateFormat),
                    limit: formatDate('2019-04-26T08:00:23.394-07:00', dateFormat),
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
                    start: formatDate('2020-08-12T15:40:48.470Z', dateFormat),
                    end: formatDate('2020-08-12T15:50:46.470Z', dateFormat),
                    limit: formatDate('2020-08-12T15:50:48.470Z', dateFormat),
                    holes: [],
                    count: 148
                },
                slicer_id: 0
            },
            {
                lastSlice: {
                    start: formatDate('2020-08-12T16:04:00.000Z', dateFormat),
                    end: formatDate('2020-08-12T16:04:59.000Z', dateFormat),
                    limit: formatDate('2020-08-12T16:05:00.000Z', dateFormat),
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
                    start: formatDate('2019-04-26T08:00:23.315-07:00', dateFormat),
                    end: formatDate('2019-04-26T08:00:23.334-07:00', dateFormat),
                    limit: formatDate('2019-04-26T08:00:23.394-07:00', dateFormat),
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
                    start: '2020-08-12T08:41:01-07:00',
                    end: '2020-08-12T08:46:01-07:00',
                    limit: '2020-08-12T08:49:01-07:00',
                    holes: [],
                    count: 58
                },
                slicer_id: 0
            },
            {
                lastSlice: {
                    start: '2020-08-12T08:49:01-07:00',
                    end: '2020-08-12T08:54:01-07:00',
                    limit: '2020-08-12T08:57:01-07:00',
                    holes: [],
                    count: 16
                },
                slicer_id: 1
            },
            {
                lastSlice: {
                    start: '2020-08-12T08:57:01-07:00',
                    end: '2020-08-12T09:02:01-07:00',
                    limit: '2020-08-12T09:05:01-07:00',
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
                    start: '2020-08-12T08:41:01-07:00',
                    end: '2020-08-12T08:46:01-07:00',
                    limit: '2020-08-12T08:53:01-07:00',
                    holes: [],
                    count: 58
                },
                slicer_id: 0
            },
            {
                lastSlice: {
                    start: '2020-08-12T08:53:01-07:00',
                    end: '2020-08-12T08:58:01-07:00',
                    limit: '2020-08-12T09:05:01-07:00',
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
