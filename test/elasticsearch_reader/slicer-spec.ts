/* eslint-disable @typescript-eslint/no-unused-vars */
import 'jest-extended';
import {
    DataEntity,
    pDelay,
    LifeCycle,
    SlicerRecoveryData,
    AnyObject,
} from '@terascope/job-components';
import moment from 'moment';
import { getESVersion } from 'elasticsearch-store';
import { SlicerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { IDType } from '../../asset/src/id_reader/interfaces';
import { getKeyArray } from '../../asset/src/id_reader/helpers';
import { dateFormatSeconds, divideRange, dateFormat } from '../../asset/src/elasticsearch_reader/elasticsearch_date_range/helpers';
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

    const evenOriginalStart = '2019-04-26T08:00:23.201-07:00';
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

    afterAll(async () => {
        await cleanupIndex(esClient, makeIndex('*'));
    });

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
            start: formatDate('2020-08-12T16:05:00.000Z', dateFormatSeconds)
        };

        const test = await makeSlicerTest({ opConfig });
        const allSlices = await test.getAllSlices();

        const hexadecimal = getKeyArray(IDType.hexadecimal);

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

    // it('slicer can enter recovery and return to the last slice state', async () => {
    //     const firstDate = makeDate(dateFormatSeconds);
    //     const middleDate = moment(firstDate).add(5, 'm');
    //     const endDate = moment(firstDate).add(10, 'm');
    //     const closingDate = moment(endDate).add(10, 's');

    //     const opConfig = {
    //         _op: 'elasticsearch_reader',
    //         date_field_name: '@timestamp',
    //         time_resolution: 's',
    //         size: 100,
    //         start: firstDate.format(dateFormatSeconds),
    //         end: closingDate.format(dateFormatSeconds),
    //         index: 'some_index',
    //         interval: '5m',
    //     };

    //     const recoveryData = [
    //         {
    //             lastSlice: {
    //                 start: middleDate.format(dateFormatSeconds),
    //                 end: endDate.format(dateFormatSeconds),
    //                 limit: closingDate.format(dateFormatSeconds),
    //                 count: 2445
    //             },
    //             slicer_id: 0
    //         }
    //     ];

    //     const expectedSlice = {
    //         start: endDate.format(dateFormatSeconds),
    //         end: closingDate.format(dateFormatSeconds),
    //         limit: closingDate.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };

    //     const test = await makeSlicerTest({ opConfig, recoveryData });

    //     const [results] = await test.createSlices();
    //     expect(results).toEqual(expectedSlice);

    //     const [results2] = await test.createSlices();
    //     expect(results2).toEqual(null);
    // });

    // it('multiple slicers can enter recovery and return to the last slice state', async () => {
    //     const firstDate = makeDate(dateFormatSeconds);
    //     const firstMiddleDate = moment(firstDate).add(5, 'm');
    //     const firstFinalDate = moment(firstDate).add(10, 'm');
    //     const secondMiddleDate = moment(firstDate).add(15, 'm');
    //     const secondFinalDate = moment(firstDate).add(20, 'm');

    //     const opConfig = {
    //         _op: 'elasticsearch_reader',
    //         date_field_name: '@timestamp',
    //         time_resolution: 's',
    //         size: 100,
    //         start: firstDate.format(dateFormatSeconds),
    //         end: secondFinalDate.format(dateFormatSeconds),
    //         index: 'some_index',
    //         interval: '5m',
    //     };

    //     const recoveryData = [
    //         {
    //             lastSlice: {
    //                 start: firstDate.format(dateFormatSeconds),
    //                 end: firstMiddleDate.format(dateFormatSeconds),
    //                 limit: firstFinalDate.format(dateFormatSeconds),
    //                 count: 2445
    //             },
    //             slicer_id: 0
    //         },
    //         {
    //             lastSlice: {
    //                 start: firstFinalDate.format(dateFormatSeconds),
    //                 end: secondMiddleDate.format(dateFormatSeconds),
    //                 limit: secondFinalDate.format(dateFormatSeconds),
    //                 count: 2445
    //             },
    //             slicer_id: 1
    //         }
    //     ];

    //     const numOfSlicers = 2;

    //     const test = await makeSlicerTest({ opConfig, numOfSlicers, recoveryData });

    //     const slicers = test.slicer();
    //     expect(slicers.slicers()).toEqual(2);

    //     const [resultsSlicer1, resultsSlicer2] = await test.createSlices();

    //     expect(resultsSlicer1).toEqual({
    //         start: firstMiddleDate.format(dateFormatSeconds),
    //         end: firstFinalDate.format(dateFormatSeconds),
    //         limit: firstFinalDate.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     });

    //     expect(resultsSlicer2).toEqual({
    //         start: secondMiddleDate.format(dateFormatSeconds),
    //         end: secondFinalDate.format(dateFormatSeconds),
    //         limit: secondFinalDate.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     });

    //     const [resultsSlicer3, resultsSlicer4] = await test.createSlices();
    //     expect(resultsSlicer3).toEqual(null);
    //     expect(resultsSlicer4).toEqual(null);
    // });

    // it('slicer can enter recovery and return to the last slice state in persistent mode', async () => {
    //     const delay: [number, moment.unitOfTime.Base] = [30, 's'];
    //     const currentDate = makeDate(dateFormatSeconds);
    //     const startDate = moment(currentDate).subtract(10, 'm');
    //     const middleDate = moment(currentDate).subtract(5, 'm');
    //     // end is delayed by setting
    //     const endingData = moment(currentDate).subtract(delay[0], delay[1]);

    //     const opConfig = {
    //         _op: 'elasticsearch_reader',
    //         date_field_name: '@timestamp',
    //         time_resolution: 's',
    //         size: 100,
    //         index: 'some_index',
    //         interval: '5m',
    //         delay: delay.join('')
    //     };

    //     const recoveryData = [
    //         {
    //             lastSlice: {
    //                 start: startDate.format(dateFormatSeconds),
    //                 end: middleDate.format(dateFormatSeconds),
    //                 limit: endingData.format(dateFormatSeconds),
    //                 count: 2445
    //             },
    //             slicer_id: 0
    //         }
    //     ];

    //     const test = await makeSlicerTest({ opConfig, recoveryData, lifecycle: 'persistent' });

    //     const expectedResult = {
    //         start: middleDate.format(dateFormatSeconds),
    //         end: endingData.format(dateFormatSeconds),
    //         limit: endingData.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };

    //     const [results] = await test.createSlices();
    //     expect(results).toEqual(expectedResult);
    // });

    // it('slicer can enter recovery and return to the last slice state when number of slicers have increased (1 => 2, even increase)', async () => {
    //     const firstDate = makeDate(dateFormatSeconds);
    //     const middleDate = moment(firstDate).add(5, 'm');
    //     const endDate = moment(firstDate).add(10, 'm');

    //     const opConfig = {
    //         _op: 'elasticsearch_reader',
    //         date_field_name: '@timestamp',
    //         time_resolution: 's',
    //         size: 100,
    //         start: firstDate.format(dateFormatSeconds),
    //         end: endDate.format(dateFormatSeconds),
    //         index: 'some_index',
    //         interval: '5m',
    //     };

    //     const recoveryData = [
    //         {
    //             lastSlice: {
    //                 start: firstDate.format(dateFormatSeconds),
    //                 end: middleDate.format(dateFormatSeconds),
    //                 limit: endDate.format(dateFormatSeconds),
    //                 count: 2445
    //             },
    //             slicer_id: 0
    //         }
    //     ];

    //     const newRange = divideRange(middleDate, endDate, 2);

    //     const expectedSlice1 = {
    //         start: newRange[0].start.format(dateFormatSeconds),
    //         end: newRange[0].limit.format(dateFormatSeconds),
    //         limit: newRange[0].limit.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };

    //     const expectedSlice2 = {
    //         start: newRange[1].start.format(dateFormatSeconds),
    //         end: newRange[1].limit.format(dateFormatSeconds),
    //         limit: newRange[1].limit.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };

    //     const test = await makeSlicerTest({ opConfig, recoveryData, numOfSlicers: 2 });

    //     const [results, results2] = await test.createSlices();
    //     expect(results).toEqual(expectedSlice1);
    //     expect(results2).toEqual(expectedSlice2);

    //     const [results3] = await test.createSlices();
    //     expect(results3).toEqual(null);
    // });

    // it('slicer can enter recovery and return to the last slice state when number of slicers have increased (3 => 5, odd increase)', async () => {
    //     const firstDate = makeDate(dateFormatSeconds);
    //     const endDate = moment(firstDate).add(20, 'm');

    //     const oldRange = divideRange(firstDate, endDate, 3);

    //     defaultClient.setSequenceData(times(30, () => ({ count: 100, '@timestamp': new Date() })));

    //     const opConfig = {
    //         _op: 'elasticsearch_reader',
    //         date_field_name: '@timestamp',
    //         time_resolution: 's',
    //         size: 10000,
    //         start: firstDate.format(dateFormatSeconds),
    //         end: endDate.format(dateFormatSeconds),
    //         index: 'some_index',
    //         interval: '5m',
    //     };

    //     const recoveryData = oldRange.map((segment, index) => {
    //         const obj = {
    //             start: moment(segment.start).format(dateFormatSeconds),
    //             end: moment(segment.start).add(2, 'm').format(dateFormatSeconds),
    //             limit: moment(segment.limit).format(dateFormatSeconds),
    //             count: 1234,
    //         };

    //         return { lastSlice: obj, slicer_id: index };
    //     });

    //     const rs1Start = moment(recoveryData[0].lastSlice.end);
    //     const rs1End = moment(recoveryData[0].lastSlice.limit);

    //     const rs2Start = moment(recoveryData[1].lastSlice.end);
    //     const rs2End = moment(recoveryData[1].lastSlice.limit);

    //     const rs3Start = moment(recoveryData[2].lastSlice.end);
    //     const rs3End = moment(recoveryData[2].lastSlice.limit);

    //     const newRangeSegment1 = divideRange(rs1Start, rs1End, 2);
    //     const newRangeSegment2 = divideRange(rs2Start, rs2End, 2);

    //     const expectedSlice1 = {
    //         start: newRangeSegment1[0].start.format(dateFormatSeconds),
    //         end: newRangeSegment1[0].limit.format(dateFormatSeconds),
    //         limit: newRangeSegment1[0].limit.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };
    //     const expectedSlice2 = {
    //         start: newRangeSegment1[1].start.format(dateFormatSeconds),
    //         end: newRangeSegment1[1].limit.format(dateFormatSeconds),
    //         limit: newRangeSegment1[1].limit.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };
    //     const expectedSlice3 = {
    //         start: newRangeSegment2[0].start.format(dateFormatSeconds),
    //         end: newRangeSegment2[0].limit.format(dateFormatSeconds),
    //         limit: newRangeSegment2[0].limit.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };
    //     const expectedSlice4 = {
    //         start: newRangeSegment2[1].start.format(dateFormatSeconds),
    //         end: newRangeSegment2[1].limit.format(dateFormatSeconds),
    //         limit: newRangeSegment2[1].limit.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };
    //     const expectedSlice5 = {
    //         start: rs3Start,
    //         end: rs3End,
    //         limit: rs3End,
    //         holes: [],
    //         count: 100
    //     };

    //     const test = await makeSlicerTest({ opConfig, recoveryData, numOfSlicers: 5 });

    //     const [results, results2, results3, results4, results5] = await test.createSlices();

    //     expect(results).toEqual(expectedSlice1);
    //     expect(moment(results?.start).isSame(expectedSlice1.start)).toBeTrue();
    //     expect(moment(results?.end).isSame(moment(expectedSlice1.end))).toBeTrue();
    //     expect(moment(results?.limit).isSame(moment(expectedSlice1.limit))).toBeTrue();

    //     expect(moment(results2?.start).isSame(expectedSlice2.start)).toBeTrue();
    //     expect(moment(results2?.end).isSame(moment(expectedSlice2.end))).toBeTrue();
    //     expect(moment(results2?.limit).isSame(moment(expectedSlice2.limit))).toBeTrue();

    //     expect(moment(results3?.start).isSame(expectedSlice3.start)).toBeTrue();
    //     expect(moment(results3?.end).isSame(moment(expectedSlice3.end))).toBeTrue();
    //     expect(moment(results3?.limit).isSame(moment(expectedSlice3.limit))).toBeTrue();

    //     expect(moment(results4?.start).isSame(expectedSlice4.start)).toBeTrue();
    //     expect(moment(results4?.end).isSame(moment(expectedSlice4.end))).toBeTrue();
    //     expect(moment(results4?.limit).isSame(moment(expectedSlice4.limit))).toBeTrue();

    //     expect(moment(results5?.start).isSame(expectedSlice5.start)).toBeTrue();
    //     expect(moment(results5?.end).isSame(moment(expectedSlice5.end))).toBeTrue();
    //     expect(moment(results5?.limit).isSame(moment(expectedSlice5.limit))).toBeTrue();

    //     const [results6] = await test.createSlices();
    //     expect(results6).toEqual(null);
    // });

    // it('slicer can enter recovery and return to the last slice state when number of slicers have decreased (2 => 1, even increase)', async () => {
    //     const firstDate = makeDate(dateFormatSeconds);
    //     const endDate = moment(firstDate).add(11, 'm');

    //     defaultClient.setSequenceData(times(30, () => ({ count: 100, '@timestamp': new Date() })));

    //     const oldRange = divideRange(firstDate, endDate, 2);

    //     const opConfig = {
    //         _op: 'elasticsearch_reader',
    //         date_field_name: '@timestamp',
    //         time_resolution: 's',
    //         size: 100,
    //         start: firstDate.format(dateFormatSeconds),
    //         end: endDate.format(dateFormatSeconds),
    //         index: 'some_index',
    //         interval: '2m',
    //     };

    //     const recoveryData = oldRange.map((segment, index) => {
    //         const obj = {
    //             start: moment(segment.start).format(dateFormatSeconds),
    //             end: moment(segment.start).add(1, 'm').format(dateFormatSeconds),
    //             limit: moment(segment.limit).format(dateFormatSeconds),
    //             count: 1234,
    //         };

    //         return { lastSlice: obj, slicer_id: index };
    //     });

    //     const hole = {
    //         start: moment(recoveryData[0].lastSlice.limit).format(dateFormat),
    //         end: moment(recoveryData[1].lastSlice.end).format(dateFormat)
    //     };

    //     const limit = moment(recoveryData[1].lastSlice.limit);

    //     // we slice 2 mins
    //     const rs1Start = moment(recoveryData[0].lastSlice.end);
    //     const rs1End = moment(rs1Start).add(2, 'm');

    //     // we slice 2 mins
    //     const rs2Start = moment(rs1End);
    //     const rs2End = moment(rs2Start).add(2, 'm');

    //     // we are up against the hole now
    //     const rs3Start = moment(rs2End);
    //     const rs3End = moment(hole.start);

    //     // we jump over the hole
    //     const rs4Start = moment(hole.end);
    //     const rs4End = moment(rs4Start).add(2, 'm');

    //     // we slice 2 mins
    //     const rs5Start = moment(rs4End);
    //     const rs5End = moment(rs5Start).add(2, 'm');

    //     // we slice 2 mins
    //     const rs6Start = moment(rs5End);

    //     const expectedSlice1 = {
    //         start: rs1Start.format(dateFormatSeconds),
    //         end: rs1End.format(dateFormatSeconds),
    //         limit: limit.format(dateFormatSeconds),
    //         holes: [hole],
    //         count: 100
    //     };
    //         // we slice 2 mins
    //     const expectedSlice2 = {
    //         start: rs2Start.format(dateFormatSeconds),
    //         end: rs2End.format(dateFormatSeconds),
    //         limit: limit.format(dateFormatSeconds),
    //         holes: [hole],
    //         count: 100
    //     };
    //         // we are up against the hole so we can drop it, internally it jumps pass the hole
    //     const expectedSlice3 = {
    //         start: rs3Start.format(dateFormatSeconds),
    //         end: rs3End.format(dateFormatSeconds),
    //         limit: limit.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };

    //     const expectedSlice4 = {
    //         start: rs4Start.format(dateFormatSeconds),
    //         end: rs4End.format(dateFormatSeconds),
    //         limit: limit.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };

    //     const expectedSlice5 = {
    //         start: rs5Start.format(dateFormatSeconds),
    //         end: rs5End.format(dateFormatSeconds),
    //         limit: limit.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };

    //     const expectedSlice6 = {
    //         start: rs6Start.format(dateFormatSeconds),
    //         end: limit.format(dateFormatSeconds),
    //         limit: limit.format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };

    //     const test = await makeSlicerTest({ opConfig, recoveryData, numOfSlicers: 1 });

    //     const [results] = await test.createSlices();

    //     expect(moment(results?.start).isSame(expectedSlice1.start)).toBeTrue();
    //     expect(moment(results?.end).isSame(moment(expectedSlice1.end))).toBeTrue();
    //     expect(moment(results?.limit).isSame(moment(expectedSlice1.limit))).toBeTrue();
    //     expect(
    //         moment(results?.holes[0].start).isSame(moment(expectedSlice1.holes[0].start))
    //     ).toBeTrue();
    //     expect(
    //         moment(results?.holes[0].end).isSame(moment(expectedSlice1.holes[0].end))
    //     ).toBeTrue();

    //     const [results2] = await test.createSlices();

    //     expect(moment(results2?.start).isSame(expectedSlice2.start)).toBeTrue();
    //     expect(moment(results2?.end).isSame(moment(expectedSlice2.end))).toBeTrue();
    //     expect(moment(results2?.limit).isSame(moment(expectedSlice2.limit))).toBeTrue();
    //     expect(
    //         moment(results2?.holes[0].start).isSame(moment(expectedSlice2.holes[0].start))
    //     ).toBeTrue();
    //     expect(
    //         moment(results2?.holes[0].end).isSame(moment(expectedSlice2.holes[0].end))
    //     ).toBeTrue();

    //     const [results3] = await test.createSlices();
    //     expect(results3).toEqual(expectedSlice3);

    //     const [results4] = await test.createSlices();
    //     expect(results4).toEqual(expectedSlice4);

    //     const [results5] = await test.createSlices();
    //     expect(results5).toEqual(expectedSlice5);

    //     const [results6] = await test.createSlices();
    //     expect(results6).toEqual(expectedSlice6);

    //     const [results7] = await test.createSlices();
    //     expect(results7).toEqual(null);
    // });

    // it('slicer can enter recovery and return to the last slice state in persistent mode with slicer changes (1 => 2)', async () => {
    //     const delay: [number, moment.unitOfTime.Base] = [30, 's'];
    //     const currentDate = makeDate(dateFormatSeconds);
    //     const startDate = moment(currentDate).subtract(10, 'm');
    //     const middleDate = moment(currentDate).subtract(5, 'm');
    //     // end is delayed by setting
    //     const endingData = moment(currentDate).subtract(delay[0], delay[1]);
    //     const startTime = Date.now();

    //     const opConfig = {
    //         _op: 'elasticsearch_reader',
    //         date_field_name: '@timestamp',
    //         time_resolution: 's',
    //         size: 100,
    //         index: 'some_index',
    //         interval: '5m',
    //         delay: delay.join('')
    //     };

    //     const recoveryData = [
    //         {
    //             lastSlice: {
    //                 start: startDate.format(dateFormatSeconds),
    //                 end: middleDate.format(dateFormatSeconds),
    //                 limit: endingData.format(dateFormatSeconds),
    //                 count: 2445
    //             },
    //             slicer_id: 0
    //         }
    //     ];

    //     const test = await makeSlicerTest({ opConfig, recoveryData, lifecycle: 'persistent' });

    //     // add the time (in seconds) took to run the tests
    //     const elapsed = Math.round((Date.now() - startTime) / 1000);
    //     const expectedResult = {
    //         start: middleDate.add(elapsed, 's').format(dateFormatSeconds),
    //         end: endingData.add(elapsed, 's').format(dateFormatSeconds),
    //         limit: endingData.add(elapsed, 's').format(dateFormatSeconds),
    //         holes: [],
    //         count: 100
    //     };

    //     const [results] = await test.createSlices();
    //     expect(results).toEqual(expectedResult);
    // });
});
