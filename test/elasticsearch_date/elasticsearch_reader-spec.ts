/* eslint-disable @typescript-eslint/no-unused-vars */
import 'jest-extended';
import {
    TestContext,
    DataEntity,
    pDelay,
    LifeCycle,
    SlicerRecoveryData,
    times,
} from '@terascope/job-components';
import path from 'path';
import moment from 'moment';
import { WorkerTestHarness, SlicerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import MockClient from '../mock_client';
import Schema from '../../asset/src/elasticsearch_reader/schema';
import { IDType } from '../../asset/src/id_reader/interfaces';
import { dateFormatSeconds, divideRange, dateFormat } from '../../asset/src/elasticsearch_reader/elasticsearch_date_range/helpers';

describe('elasticsearch_reader', () => {
    const assetDir = path.join(__dirname, '../..');
    let harness: WorkerTestHarness | SlicerTestHarness;
    let clients: any;
    let defaultClient: MockClient;

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

    afterEach(async () => {
        if (harness) {
            harness.events.emit('worker:shutdown');
            await harness.shutdown();
        }
    });

    function makeDate(format: string) {
        return moment(moment().format(format));
    }

    async function makeFetcherTest(config: any) {
        harness = WorkerTestHarness.testFetcher(config, { assetDir, clients });
        await harness.initialize();
        return harness;
    }

    async function getMeta(test: SlicerTestHarness) {
        return test.context.apis.executionContext.getMetadata('elasticsearch_reader');
    }

    interface EventHook {
        event: string;
        fn: (event?: any) => void;
    }

    interface SlicerTestArgs {
        opConfig: any;
        numOfSlicers?: number;
        recoveryData?: SlicerRecoveryData[];
        eventHook?: EventHook;
        lifecycle?: LifeCycle;
    }

    async function makeSlicerTest({
        opConfig,
        numOfSlicers = 1,
        recoveryData,
        eventHook,
        lifecycle = 'once'
    }: SlicerTestArgs) {
        const job = newTestJobConfig({
            analytics: true,
            slicers: numOfSlicers,
            lifecycle,
            operations: [
                opConfig,
                {
                    _op: 'noop'
                }
            ],
        });
        harness = new SlicerTestHarness(job, { assetDir, clients });
        if (eventHook) harness.events.on(eventHook.event, eventHook.fn);
        await harness.initialize(recoveryData);
        return harness;
    }

    function getSchema() {
        const context = new TestContext('es-reader');
        const schema = new Schema(context);
        return schema.build();
    }

    describe('schema', () => {
        it('schema function returns on object, formatted to be used by convict', async () => {
            const opName = 'elasticsearch_reader';
            const testOpConfig = {
                _op: opName,
                index: 'some-index',
                date_field_name: 'created'
            };
            const test = await makeFetcherTest(testOpConfig);
            const {
                opConfig: {
                    connection,
                    size,
                    interval,
                    delay,
                    subslice_by_key: subslice,
                    time_resolution: resolution
                }
            } = test.getOperation(opName);

            expect(connection).toEqual('default');
            expect(size).toEqual(5000);
            expect(interval).toEqual('auto');
            expect(delay).toEqual('30s');
            expect(subslice).toEqual(false);
            expect(resolution).toEqual('s');
        });

        it('can geo validatie', async () => {
            const schema = getSchema();
            const geoPointValidation = schema.geo_box_top_left.format;
            const validGeoDistance = schema.geo_distance.format;
            const geoSortOrder = schema.geo_sort_order.format;

            expect(() => geoPointValidation(null)).not.toThrowError();
            expect(() => validGeoDistance(null)).not.toThrowError();
            expect(() => geoSortOrder(null)).not.toThrowError();

            expect(() => geoPointValidation(19.1234)).toThrowError('Invalid geo_point, must be a string IF specified');
            expect(() => geoPointValidation('19.1234')).toThrowError('Invalid geo_point, received 19.1234');
            expect(() => geoPointValidation('190.1234,85.2134')).toThrowError('Invalid latitude parameter, was given 190.1234, should be >= -90 and <= 90');
            expect(() => geoPointValidation('80.1234,185.2134')).toThrowError('Invalid longitutde parameter, was given 185.2134, should be >= -180 and <= 180');
            expect(() => geoPointValidation('80.1234,-155.2134')).not.toThrowError();

            expect(() => validGeoDistance(19.1234)).toThrowError('Invalid geo_distance parameter, must be a string IF specified');
            expect(() => validGeoDistance(' ')).toThrowError('Invalid geo_distance paramter, is formatted incorrectly');
            expect(() => validGeoDistance('200something')).toThrowError('Invalid unit type, did not have a proper unit of measuerment (ie m, km, yd, ft)');
            expect(() => validGeoDistance('200km')).not.toThrowError();

            expect(() => geoSortOrder(1234)).toThrowError('Invalid geo_sort_order parameter, must be a string IF specified');
            expect(() => geoSortOrder('hello')).toThrowError('If geo_sort_order is specified it must be either "asc" or "desc"');
            expect(() => geoSortOrder('asc')).not.toThrowError();
        });

        it('subslice_by_key configuration validation', async () => {
            expect.hasAssertions();
            const errorString = 'If subslice_by_key is set to true, the field parameter of the documents must also be set';
            const badOP = { subslice_by_key: true };
            const goodOP = { subslice_by_key: true, field: 'events-' };
            const otherGoodOP = { subslice_by_key: false, other: 'events-' };
            // NOTE: geo self validations are tested in elasticsearch_api module

            const testOpConfig = {
                _op: 'elasticsearch_reader',
                index: 'some-index',
                date_field_name: 'created'
            };

            await expect(makeFetcherTest(Object.assign({}, testOpConfig, badOP)))
                .rejects
                .toThrowError(errorString);

            const goodOp = await makeFetcherTest(Object.assign({}, testOpConfig, goodOP));
            expect(goodOp).toBeDefined();

            const goodOp2 = await makeFetcherTest(Object.assign({}, testOpConfig, otherGoodOP));
            expect(goodOp2).toBeDefined();
        });
    });

    describe('slicer', () => {
        it('can create a slicer', async () => {
            const opConfig = {
                _op: 'elasticsearch_reader',
                time_resolution: 's',
                date_field_name: '@timestamp',
                size: 50,
                index: 'someindex',
                interval: '12hrs',
                start: new Date().getTime(),
                end: new Date().getTime()
            };

            const test = await makeSlicerTest({ opConfig });
            const slicer = test.slicer();

            expect(slicer.slicers()).toEqual(1);
        });

        it('can create multiple slicers', async () => {
            const opConfig = {
                _op: 'elasticsearch_reader',
                time_resolution: 's',
                date_field_name: '@timestamp',
                size: 50,
                index: 'someindex',
                interval: '12hrs',
                start: new Date().getTime(),
                end: new Date().getTime()
            };
            const numOfSlicers = 2;
            const test = await makeSlicerTest({ opConfig, numOfSlicers });
            const slicer = test.slicer();

            expect(slicer.slicers()).toEqual(2);
        });

        it('slicers will throw if date_field_name does not exist on docs in the index', async () => {
            expect.hasAssertions();

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: 'date',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '2hrs',
                start: '2015-08-25T00:00:00',
                end: '2015-08-25T00:02:00'
            };
            const errMsg = 'Invalid date_field_name: "date" for index: someindex, field does not exist on record';

            try {
                await makeSlicerTest({ opConfig });
            } catch (err) {
                expect(err.message).toStartWith(errMsg);
            }
        });

        it('slicers will emit updated operations for start and end', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const laterDate = moment(firstDate).add(5, 'm');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '2hrs'
            };

            const opConfig2 = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '2hrs',
                start: firstDate.format()
            };

            const opConfig3 = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '2hrs',
                end: moment(laterDate).add(1, 's').format()
            };

            const opConfig4 = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '2hrs',
                start: firstDate.format(),
                end: moment(laterDate).add(1, 's').format()
            };

            async function waitForUpdate(config: any, endDate?: any) {
                defaultClient.setSequenceData([{ '@timestamp': firstDate }, { '@timestamp': endDate || laterDate }]);

                return makeSlicerTest({
                    opConfig: config,
                });
            }

            const test1 = await waitForUpdate(opConfig, firstDate);
            const update1 = await getMeta(test1);

            expect(update1.start).toEqual(firstDate.format());
            expect(update1.end).toEqual(moment(firstDate).add(1, 's').format());
            expect(update1.interval).toEqual([1, 's']);

            await test1.shutdown();

            const test2 = await waitForUpdate(opConfig2);
            const update2 = await getMeta(test2);

            expect(update2.start).toEqual(firstDate.format());
            expect(update2.end).toEqual(moment(firstDate).add(5, 'm').add(1, 's').format());
            expect(update1.interval).toEqual([1, 's']);

            await test2.shutdown();

            const test3 = await waitForUpdate(opConfig3);
            const update3 = await getMeta(test3);

            expect(update3.start).toEqual(firstDate.format());
            expect(update3.end).toEqual(moment(firstDate).add(5, 'm').add(1, 's').format());
            expect(update1.interval).toEqual([1, 's']);

            await test3.shutdown();

            const test4 = await waitForUpdate(opConfig4);
            const update4 = await getMeta(test4);

            expect(update4.start).toEqual(firstDate.format());
            expect(update4.end).toEqual(moment(firstDate).add(5, 'm').add(1, 's').format());
            expect(update1.interval).toEqual([1, 's']);

            await test4.shutdown();
        });

        it('will convert auto to proper interval and update the opConfig', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const laterDate = moment(firstDate).add(5, 'm');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: 'auto',
                start: firstDate.format(),
                end: moment(laterDate).add(1, 's').format()
            };

            async function waitForUpdate(config: any) {
                const sTest = await makeSlicerTest({
                    opConfig: config,
                });
                await pDelay(100);
                return sTest;
            }

            const test = await waitForUpdate(opConfig);
            const update = await getMeta(test);

            expect(update.interval).toEqual([301, 's']);
        });

        it('slicer will not error out if query returns no results', async () => {
            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '2hrs',
                query: 'some:luceneQueryWithNoResults'
            };

            // setting sequence data to an empty array to simulate a query with no results
            defaultClient.setSequenceData([]);
            const test = await makeSlicerTest({ opConfig });
            const results = await test.createSlices();

            expect(results).toEqual([null]);
        });

        it('slicer can produce date slices', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const laterDate = moment(firstDate).add(5, 'm');
            const closingDate = moment(laterDate).add(1, 's');
            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '2hrs'
            };

            // the last two data are not important here, they just need to exists as a response
            defaultClient.setSequenceData([
                { '@timestamp': firstDate },
                { '@timestamp': laterDate },
                { '@timestamp': laterDate },
                { '@timestamp': laterDate },
            ]);

            const test = await makeSlicerTest({ opConfig });
            const [results] = await test.createSlices();

            expect(results?.start).toEqual(firstDate.format());
            expect(results?.end).toEqual(closingDate.format());
            expect(results?.count).toEqual(100);

            const [results2] = await test.createSlices();
            expect(results2).toEqual(null);
        });

        it('can run a persistent reader', async () => {
            const delay: [number, moment.unitOfTime.Base] = [100, 'ms'];
            const start = makeDate(dateFormat);
            const delayedBoundary = moment(start).subtract(delay[0], delay[1]);

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 'ms',
                size: 100,
                index: 'someindex',
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

        it('can run a persistent reader with multiple slicers', async () => {
            const delay: [number, moment.unitOfTime.Base] = [100, 'ms'];

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 'ms',
                size: 100,
                index: 'someindex',
                interval: '200ms',
                delay: delay.join('')
            };

            defaultClient.setSequenceData(times(50, () => ({ count: 100, '@timestamp': new Date() })));

            const test = await makeSlicerTest({ opConfig, lifecycle: 'persistent', numOfSlicers: 2 });

            await pDelay(210);

            const [results, results2] = await test.createSlices();

            expect(results).not.toEqual(null);
            expect(results2).not.toEqual(null);
            expect(results?.limit).toEqual(results2?.start);
            expect(moment(results2?.limit).diff(results?.start)).toEqual(200);

            const [results3, results4] = await test.createSlices();

            expect(results3).toEqual(null);
            expect(results4).toEqual(null);

            await pDelay(210);

            const [results5, results6] = await test.createSlices();

            expect(results5).not.toEqual(null);
            expect(results6).not.toEqual(null);
            expect(results5?.limit).toEqual(results6?.start);
            expect(results5?.start).toEqual(results2?.limit);
            expect(moment(results6?.limit).diff(results5?.start)).toEqual(200);

            const [results7, results8] = await test.createSlices();

            expect(results7).toEqual(null);
            expect(results8).toEqual(null);

            const [results9, results10] = await test.createSlices();

            expect(results9).toEqual(null);
            expect(results10).toEqual(null);
        });

        it('persistent reader will work correctly when no data is present', async () => {
            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '100ms',
                delay: '3s'
            };
            // we do this to simulate no return data
            defaultClient.setSequenceData([]);
            const test = await makeSlicerTest({ opConfig, lifecycle: 'persistent' });

            const [results] = await test.createSlices();
            expect(results).toBeDefined();
            expect(results?.start).toBeDefined();
            expect(results?.end).toBeDefined();
            expect(results?.count).toBeDefined();

            const [results2] = await test.createSlices();
            expect(results2).toEqual(null);

            const [results3] = await test.createSlices();
            expect(results3).toEqual(null);

            await pDelay(110);

            const [results4] = await test.createSlices();
            expect(results4).toBeDefined();
            expect(results4?.start).toBeDefined();
            expect(results4?.end).toBeDefined();
            expect(results4?.count).toBeDefined();

            const [results5] = await test.createSlices();
            expect(results5).toEqual(null);

            const [results6] = await test.createSlices();
            expect(results6).toEqual(null);

            await pDelay(110);

            const [results7] = await test.createSlices();
            expect(results7).toBeDefined();
            expect(results7?.start).toBeDefined();
            expect(results7?.end).toBeDefined();
            expect(results7?.count).toBeDefined();

            const [results8] = await test.createSlices();
            expect(results8).toEqual(null);

            const [results9] = await test.createSlices();
            expect(results9).toEqual(null);
        });

        it('can run a persistent reader with recoveryData with no lastSlice', async () => {
            const delay: [number, moment.unitOfTime.Base] = [100, 'ms'];
            const start = makeDate(dateFormat);
            const delayedBoundary = moment(start).subtract(delay[0], delay[1]);

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 'ms',
                size: 100,
                index: 'someindex',
                interval: '100ms',
                delay: delay.join('')
            };

            const recoveryData = [
                {
                    lastSlice: undefined,
                    slicer_id: 0
                }
            ];

            const test = await makeSlicerTest({ opConfig, lifecycle: 'persistent', recoveryData });

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
            const firstDate = makeDate(dateFormatSeconds);
            const middleDate = moment(firstDate).add(5, 'm');
            const endDate = moment(firstDate).add(10, 'm');
            const closingDate = moment(endDate).add(1, 's');
            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 50,
                index: 'someindex',
                interval: '2hrs',
            };

            // first two objects are consumed for determining start and end dates,
            // a middleDate is used in recursion to split in half, so it needs two

            defaultClient.setSequenceData([
                { '@timestamp': firstDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { '@timestamp': firstDate, count: 100 },
                { '@timestamp': middleDate, count: 50 },
                { '@timestamp': middleDate, count: 50 },
                { '@timestamp': endDate, count: 50 }
            ]);

            let hasRecursed = false;

            function hasRecursedEvent() {
                hasRecursed = true;
            }

            const eventHook = { event: 'slicer:slice:recursion', fn: hasRecursedEvent };
            const test = await makeSlicerTest({
                opConfig,
                eventHook
            });

            const [results] = await test.createSlices();

            expect(results?.start).toEqual(firstDate.format());
            expect(results?.end).toEqual(middleDate.format());
            expect(results?.count).toEqual(50);

            const [results2] = await test.createSlices();

            expect(hasRecursed).toEqual(true);
            expect(results2?.start).toEqual(middleDate.format());
            expect(results2?.end).toEqual(closingDate.format());
            expect(results2?.count).toEqual(50);

            const results3 = await test.createSlices();
            expect(results3).toEqual([null]);
        });

        it('slicer can do a simple expansion of date slices up to find data', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const endDate = moment(firstDate).add(10, 'm');
            const closingDate = moment(endDate).add(1, 's');
            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '5m',
            };

            // first two objects are consumed for determining start and end dates,
            // a middleDate is used in recursion to expand,
            defaultClient.setSequenceData([
                { '@timestamp': firstDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { count: 0 },
                { count: 100 },
                { count: 100 },
                { count: 100 }
            ]);

            let hasExpanded = false;

            function hasExpandedFn() {
                hasExpanded = true;
            }

            const eventHook = { event: 'slicer:slice:range_expansion', fn: hasExpandedFn };

            const test = await makeSlicerTest({
                opConfig,
                eventHook
            });
            const [results] = await test.createSlices();

            expect(results?.start).toEqual(firstDate.format());
            expect(results?.end).toEqual(endDate.format());
            expect(results?.count).toEqual(100);

            const [results2] = await test.createSlices();

            expect(hasExpanded).toEqual(true);
            expect(results2?.start).toEqual(endDate.format());
            expect(results2?.end).toEqual(closingDate.format());
            expect(results2?.count).toEqual(100);

            const results3 = await test.createSlices();
            expect(results3).toEqual([null]);
        });

        it('slicer can do an expansion of date slices up to find data even when none is returned', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const endDate = moment(firstDate).add(10, 'm');
            const closingDate = moment(endDate).add(1, 's');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '5m',
            };

            // first two objects are consumed for determining start and end dates,
            // a middleDate is used in recursion to expand,
            defaultClient.setSequenceData([
                { '@timestamp': firstDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { count: 0 },
                { count: 0 },
                { count: 0 },
                { count: 0 }
            ]);

            let hasExpanded = false;
            function hasExpandedFn() {
                hasExpanded = true;
            }

            const eventHook = { event: 'slicer:slice:range_expansion', fn: hasExpandedFn };
            const test = await makeSlicerTest({
                opConfig,
                eventHook
            });
            const [results] = await test.createSlices();

            expect(results?.start).toEqual(firstDate.format());
            expect(results?.end).toEqual(closingDate.format());
            expect(results?.count).toEqual(0);

            expect(hasExpanded).toEqual(true);

            const results2 = await test.createSlices();
            expect(results2).toEqual([null]);
        });

        it('slicer can do expansion of date slices with large slices', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const middleDate = moment(firstDate).add(5, 'm');
            const endDate = moment(firstDate).add(10, 'm');
            const closingDate = moment(endDate).add(1, 's');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '5m',
            };
            // first two objects are consumed for determining start and end dates,
            // the count of zero hits the expansion code, then it hits the 150 which is
            // above the size limit so it runs another recursive query
            defaultClient.setSequenceData([
                { '@timestamp': firstDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { count: 0 },
                { count: 150 },
                { count: 100 },
                { count: 100 },
                { count: 100 }
            ]);

            let hasExpanded = false;
            function hasExpandedFn() {
                hasExpanded = true;
            }
            const eventHook = { event: 'slicer:slice:range_expansion', fn: hasExpandedFn };
            const test = await makeSlicerTest({ opConfig, eventHook });
            const [results] = await test.createSlices();

            expect(results?.start).toEqual(firstDate.format());
            expect(moment(results?.end).isBetween(middleDate, endDate)).toEqual(true);
            expect(results?.count).toEqual(100);

            const [results2] = await test.createSlices();
            expect(hasExpanded).toEqual(true);
            expect(moment(results2?.start).isBetween(middleDate, endDate)).toEqual(true);
            expect(results2?.end).toEqual(closingDate.format());
            expect(results2?.count).toEqual(100);

            const results3 = await test.createSlices();
            expect(results3).toEqual([null]);
        });

        it('slicer can expand date slices properly in uneven data distribution', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const midDate = moment(firstDate).add(8, 'm');
            const endDate = moment(firstDate).add(16, 'm');
            const closingDate = moment(endDate).add(1, 's');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '3m',
            };

            // first two objects are consumed for determining start and end dates,
            // the count of zero hits the expansion code, then it hits the 150 which is
            // above the size limit so it runs another recursive query
            defaultClient.setSequenceData([
                { '@timestamp': firstDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { count: 0 },
                { count: 150 },
                { count: 0 },
                { count: 100 },
                { count: 100 },
                { count: 100 },
                { count: 100 }
            ]);

            let hasExpanded = false;
            function hasExpandedFn() {
                hasExpanded = true;
            }

            const eventHook = { event: 'slicer:slice:range_expansion', fn: hasExpandedFn };
            const test = await makeSlicerTest({ opConfig, eventHook });
            const [results] = await test.createSlices();

            expect(results?.start).toEqual(firstDate.format());
            expect(moment(results?.end).isBetween(firstDate, midDate)).toEqual(true);
            expect(results?.count).toEqual(100);

            const [results2] = await test.createSlices();

            expect(moment(results2?.end).isBetween(midDate, endDate)).toEqual(true);
            expect(hasExpanded).toEqual(true);
            expect(results2?.count).toEqual(100);

            const [results3] = await test.createSlices();

            expect(moment(results3?.end).isBetween(midDate, endDate)).toEqual(true);

            const [results4] = await test.createSlices();
            expect(results4?.end).toEqual(closingDate.format());

            const results5 = await test.createSlices();
            expect(results5).toEqual([null]);
        });

        it('slicer can will recurse down to smallest factor using "s" format', async () => {
            const firstDateMS = makeDate(dateFormatSeconds).toISOString();
            const firstDateS = moment(firstDateMS);
            const closingDateS = moment(firstDateS).add(1, 's');
            const endDate = moment(firstDateMS).add(5, 'm');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 10,
                index: 'someindex',
                interval: '5m',
            };

            defaultClient.deepRecursiveResponseCount = 100;
            // first two objects are consumed for determining start and end dates,
            // a middleDate is used in recursion to expand,
            defaultClient.setSequenceData([
                { '@timestamp': firstDateS, count: 100 },
                { '@timestamp': endDate, count: 100 },
            ]);

            const test = await makeSlicerTest({ opConfig });
            const [resultsS] = await test.createSlices();

            expect(resultsS?.start).toEqual(firstDateS.format());
            expect(resultsS?.end).toEqual(closingDateS.format());
            expect(resultsS?.count).toEqual(100);
        });

        it('slicer can will recurse down to smallest factor using "ms" format', async () => {
            const firstDateMS = makeDate(dateFormatSeconds).toISOString();
            const firstDateS = moment(firstDateMS);
            const closingDateMS = moment(firstDateMS).add(1, 'ms');
            const endDate = moment(firstDateMS).add(5, 'm');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 'ms',
                size: 10,
                index: 'someindex',
                interval: '5m',
            };

            defaultClient.deepRecursiveResponseCount = 100;
            // first two objects are consumed for determining start and end dates,
            // a middleDate is used in recursion to expand,
            defaultClient.setSequenceData([
                { '@timestamp': firstDateS, count: 100 },
                { '@timestamp': endDate, count: 100 }
            ]);

            // Need to run them seperatly so they get a different client
            const test = await makeSlicerTest({ opConfig });
            const [resultsMS] = await test.createSlices();

            const startMsIsSame = moment(resultsMS?.start).isSame(moment(firstDateMS));
            const endMsIsSame = moment(resultsMS?.end).isSame(moment(closingDateMS));

            expect(startMsIsSame).toEqual(true);
            expect(endMsIsSame).toEqual(true);
            expect(resultsMS?.count).toEqual(100);
        });

        it('slicer can will recurse down to smallest factor and subslice by key', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const closingDate = moment(firstDate).add(1, 's');
            const endDate = moment(firstDate).add(5, 'm');
            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 10,
                index: 'someindex',
                interval: '5m',
                subslice_by_key: true,
                subslice_key_threshold: 50,
                key_type: IDType.hexadecimal,
                field: 'test'
            };
            const hexadecimal = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'];
            defaultClient.deepRecursiveResponseCount = 10;
            // first two objects are consumed for determining start and end dates,
            // a middleDate is used in recursion to expand,
            defaultClient.setSequenceData([
                { '@timestamp': firstDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
                { '@timestamp': endDate, count: 100 },
            ]);

            const test = await makeSlicerTest({ opConfig });
            const results = await test.createSlices();

            hexadecimal.forEach((char) => {
                const subslice = results.find((s) => s?.wildcard?.value === `${char}*`);
                expect(subslice).not.toBeUndefined();
                expect(subslice!.start === firstDate.format()).toEqual(true);
                expect(subslice!.end === closingDate.format()).toEqual(true);
            });
        });

        it('slicer can enter recovery and return to the last slice state', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const middleDate = moment(firstDate).add(5, 'm');
            const endDate = moment(firstDate).add(10, 'm');
            const closingDate = moment(endDate).add(10, 's');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                start: firstDate.format(dateFormatSeconds),
                end: closingDate.format(dateFormatSeconds),
                index: 'someindex',
                interval: '5m',
            };

            const recoveryData = [
                {
                    lastSlice: {
                        start: middleDate.format(dateFormatSeconds),
                        end: endDate.format(dateFormatSeconds),
                        limit: closingDate.format(dateFormatSeconds),
                        count: 2445
                    },
                    slicer_id: 0
                }
            ];

            const expectedSlice = {
                start: endDate.format(dateFormatSeconds),
                end: closingDate.format(dateFormatSeconds),
                limit: closingDate.format(dateFormatSeconds),
                holes: [],
                count: 100
            };

            const test = await makeSlicerTest({ opConfig, recoveryData });

            const [results] = await test.createSlices();
            expect(results).toEqual(expectedSlice);

            const [results2] = await test.createSlices();
            expect(results2).toEqual(null);
        });

        it('multiple slicers can enter recovery and return to the last slice state', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const firstMiddleDate = moment(firstDate).add(5, 'm');
            const firstFinalDate = moment(firstDate).add(10, 'm');
            const secondMiddleDate = moment(firstDate).add(15, 'm');
            const secondFinalDate = moment(firstDate).add(20, 'm');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                start: firstDate.format(dateFormatSeconds),
                end: secondFinalDate.format(dateFormatSeconds),
                index: 'someindex',
                interval: '5m',
            };

            const recoveryData = [
                {
                    lastSlice: {
                        start: firstDate.format(dateFormatSeconds),
                        end: firstMiddleDate.format(dateFormatSeconds),
                        limit: firstFinalDate.format(dateFormatSeconds),
                        count: 2445
                    },
                    slicer_id: 0
                },
                {
                    lastSlice: {
                        start: firstFinalDate.format(dateFormatSeconds),
                        end: secondMiddleDate.format(dateFormatSeconds),
                        limit: secondFinalDate.format(dateFormatSeconds),
                        count: 2445
                    },
                    slicer_id: 1
                }
            ];

            const numOfSlicers = 2;

            const test = await makeSlicerTest({ opConfig, numOfSlicers, recoveryData });

            const slicers = test.slicer();
            expect(slicers.slicers()).toEqual(2);

            const [resultsSlicer1, resultsSlicer2] = await test.createSlices();

            expect(resultsSlicer1).toEqual({
                start: firstMiddleDate.format(dateFormatSeconds),
                end: firstFinalDate.format(dateFormatSeconds),
                limit: firstFinalDate.format(dateFormatSeconds),
                holes: [],
                count: 100
            });

            expect(resultsSlicer2).toEqual({
                start: secondMiddleDate.format(dateFormatSeconds),
                end: secondFinalDate.format(dateFormatSeconds),
                limit: secondFinalDate.format(dateFormatSeconds),
                holes: [],
                count: 100
            });

            const [resultsSlicer3, resultsSlicer4] = await test.createSlices();
            expect(resultsSlicer3).toEqual(null);
            expect(resultsSlicer4).toEqual(null);
        });

        it('slicer can enter recovery and return to the last slice state in persistent mode', async () => {
            const delay: [number, moment.unitOfTime.Base] = [30, 's'];
            const currentDate = makeDate(dateFormatSeconds);
            const startDate = moment(currentDate).subtract(10, 'm');
            const middleDate = moment(currentDate).subtract(5, 'm');
            // end is delayed by setting
            const endingData = moment(currentDate).subtract(delay[0], delay[1]);

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '5m',
                delay: delay.join('')
            };

            const recoveryData = [
                {
                    lastSlice: {
                        start: startDate.format(dateFormatSeconds),
                        end: middleDate.format(dateFormatSeconds),
                        limit: endingData.format(dateFormatSeconds),
                        count: 2445
                    },
                    slicer_id: 0
                }
            ];

            const test = await makeSlicerTest({ opConfig, recoveryData, lifecycle: 'persistent' });

            const expectedResult = {
                start: middleDate.format(dateFormatSeconds),
                end: endingData.format(dateFormatSeconds),
                limit: endingData.format(dateFormatSeconds),
                holes: [],
                count: 100
            };

            const [results] = await test.createSlices();
            expect(results).toEqual(expectedResult);
        });

        it('slicer can enter recovery and return to the last slice state when number of slicers have increased (1 => 2, even increase)', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const middleDate = moment(firstDate).add(5, 'm');
            const endDate = moment(firstDate).add(10, 'm');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                start: firstDate.format(dateFormatSeconds),
                end: endDate.format(dateFormatSeconds),
                index: 'someindex',
                interval: '5m',
            };

            const recoveryData = [
                {
                    lastSlice: {
                        start: firstDate.format(dateFormatSeconds),
                        end: middleDate.format(dateFormatSeconds),
                        limit: endDate.format(dateFormatSeconds),
                        count: 2445
                    },
                    slicer_id: 0
                }
            ];

            const newRange = divideRange(middleDate, endDate, 2);

            const expectedSlice1 = {
                start: newRange[0].start.format(dateFormatSeconds),
                end: newRange[0].limit.format(dateFormatSeconds),
                limit: newRange[0].limit.format(dateFormatSeconds),
                holes: [],
                count: 100
            };

            const expectedSlice2 = {
                start: newRange[1].start.format(dateFormatSeconds),
                end: newRange[1].limit.format(dateFormatSeconds),
                limit: newRange[1].limit.format(dateFormatSeconds),
                holes: [],
                count: 100
            };

            const test = await makeSlicerTest({ opConfig, recoveryData, numOfSlicers: 2 });

            const [results, results2] = await test.createSlices();
            expect(results).toEqual(expectedSlice1);
            expect(results2).toEqual(expectedSlice2);

            const [results3] = await test.createSlices();
            expect(results3).toEqual(null);
        });

        it('slicer can enter recovery and return to the last slice state when number of slicers have increased (3 => 5, odd increase)', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const endDate = moment(firstDate).add(20, 'm');

            const oldRange = divideRange(firstDate, endDate, 3);

            defaultClient.setSequenceData(times(30, () => ({ count: 100, '@timestamp': new Date() })));

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 10000,
                start: firstDate.format(dateFormatSeconds),
                end: endDate.format(dateFormatSeconds),
                index: 'someindex',
                interval: '5m',
            };

            const recoveryData = oldRange.map((segment, index) => {
                const obj = {
                    start: moment(segment.start).format(dateFormatSeconds),
                    end: moment(segment.start).add(2, 'm').format(dateFormatSeconds),
                    limit: moment(segment.limit).format(dateFormatSeconds),
                    count: 1234,
                };

                return { lastSlice: obj, slicer_id: index };
            });

            const rs1Start = moment(recoveryData[0].lastSlice.end);
            const rs1End = moment(recoveryData[0].lastSlice.limit);

            const rs2Start = moment(recoveryData[1].lastSlice.end);
            const rs2End = moment(recoveryData[1].lastSlice.limit);

            const rs3Start = moment(recoveryData[2].lastSlice.end);
            const rs3End = moment(recoveryData[2].lastSlice.limit);

            const newRangeSegment1 = divideRange(rs1Start, rs1End, 2);
            const newRangeSegment2 = divideRange(rs2Start, rs2End, 2);

            const expectedSlice1 = {
                start: newRangeSegment1[0].start.format(dateFormatSeconds),
                end: newRangeSegment1[0].limit.format(dateFormatSeconds),
                limit: newRangeSegment1[0].limit.format(dateFormatSeconds),
                holes: [],
                count: 100
            };
            const expectedSlice2 = {
                start: newRangeSegment1[1].start.format(dateFormatSeconds),
                end: newRangeSegment1[1].limit.format(dateFormatSeconds),
                limit: newRangeSegment1[1].limit.format(dateFormatSeconds),
                holes: [],
                count: 100
            };
            const expectedSlice3 = {
                start: newRangeSegment2[0].start.format(dateFormatSeconds),
                end: newRangeSegment2[0].limit.format(dateFormatSeconds),
                limit: newRangeSegment2[0].limit.format(dateFormatSeconds),
                holes: [],
                count: 100
            };
            const expectedSlice4 = {
                start: newRangeSegment2[1].start.format(dateFormatSeconds),
                end: newRangeSegment2[1].limit.format(dateFormatSeconds),
                limit: newRangeSegment2[1].limit.format(dateFormatSeconds),
                holes: [],
                count: 100
            };
            const expectedSlice5 = {
                start: rs3Start,
                end: rs3End,
                limit: rs3End,
                holes: [],
                count: 100
            };

            const test = await makeSlicerTest({ opConfig, recoveryData, numOfSlicers: 5 });

            const [results, results2, results3, results4, results5] = await test.createSlices();

            expect(results).toEqual(expectedSlice1);
            expect(moment(results?.start).isSame(expectedSlice1.start)).toBeTrue();
            expect(moment(results?.end).isSame(moment(expectedSlice1.end))).toBeTrue();
            expect(moment(results?.limit).isSame(moment(expectedSlice1.limit))).toBeTrue();

            expect(moment(results2?.start).isSame(expectedSlice2.start)).toBeTrue();
            expect(moment(results2?.end).isSame(moment(expectedSlice2.end))).toBeTrue();
            expect(moment(results2?.limit).isSame(moment(expectedSlice2.limit))).toBeTrue();

            expect(moment(results3?.start).isSame(expectedSlice3.start)).toBeTrue();
            expect(moment(results3?.end).isSame(moment(expectedSlice3.end))).toBeTrue();
            expect(moment(results3?.limit).isSame(moment(expectedSlice3.limit))).toBeTrue();

            expect(moment(results4?.start).isSame(expectedSlice4.start)).toBeTrue();
            expect(moment(results4?.end).isSame(moment(expectedSlice4.end))).toBeTrue();
            expect(moment(results4?.limit).isSame(moment(expectedSlice4.limit))).toBeTrue();

            expect(moment(results5?.start).isSame(expectedSlice5.start)).toBeTrue();
            expect(moment(results5?.end).isSame(moment(expectedSlice5.end))).toBeTrue();
            expect(moment(results5?.limit).isSame(moment(expectedSlice5.limit))).toBeTrue();

            const [results6] = await test.createSlices();
            expect(results6).toEqual(null);
        });

        it('slicer can enter recovery and return to the last slice state when number of slicers have decreased (2 => 1, even increase)', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const endDate = moment(firstDate).add(11, 'm');

            defaultClient.setSequenceData(times(30, () => ({ count: 100, '@timestamp': new Date() })));

            const oldRange = divideRange(firstDate, endDate, 2);

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                start: firstDate.format(dateFormatSeconds),
                end: endDate.format(dateFormatSeconds),
                index: 'someindex',
                interval: '2m',
            };

            const recoveryData = oldRange.map((segment, index) => {
                const obj = {
                    start: moment(segment.start).format(dateFormatSeconds),
                    end: moment(segment.start).add(1, 'm').format(dateFormatSeconds),
                    limit: moment(segment.limit).format(dateFormatSeconds),
                    count: 1234,
                };

                return { lastSlice: obj, slicer_id: index };
            });

            const hole = {
                start: moment(recoveryData[0].lastSlice.limit).format(dateFormat),
                end: moment(recoveryData[1].lastSlice.end).format(dateFormat)
            };

            const limit = moment(recoveryData[1].lastSlice.limit);

            // we slice 2 mins
            const rs1Start = moment(recoveryData[0].lastSlice.end);
            const rs1End = moment(rs1Start).add(2, 'm');

            // we slice 2 mins
            const rs2Start = moment(rs1End);
            const rs2End = moment(rs2Start).add(2, 'm');

            // we are up against the hole now
            const rs3Start = moment(rs2End);
            const rs3End = moment(hole.start);

            // we jump over the hole
            const rs4Start = moment(hole.end);
            const rs4End = moment(rs4Start).add(2, 'm');

            // we slice 2 mins
            const rs5Start = moment(rs4End);
            const rs5End = moment(rs5Start).add(2, 'm');

            // we slice 2 mins
            const rs6Start = moment(rs5End);

            const expectedSlice1 = {
                start: rs1Start.format(dateFormatSeconds),
                end: rs1End.format(dateFormatSeconds),
                limit: limit.format(dateFormatSeconds),
                holes: [hole],
                count: 100
            };
            // we slice 2 mins
            const expectedSlice2 = {
                start: rs2Start.format(dateFormatSeconds),
                end: rs2End.format(dateFormatSeconds),
                limit: limit.format(dateFormatSeconds),
                holes: [hole],
                count: 100
            };
            // we are up against the hole so we can drop it, internally it jumps pass the hole
            const expectedSlice3 = {
                start: rs3Start.format(dateFormatSeconds),
                end: rs3End.format(dateFormatSeconds),
                limit: limit.format(dateFormatSeconds),
                holes: [],
                count: 100
            };

            const expectedSlice4 = {
                start: rs4Start.format(dateFormatSeconds),
                end: rs4End.format(dateFormatSeconds),
                limit: limit.format(dateFormatSeconds),
                holes: [],
                count: 100
            };

            const expectedSlice5 = {
                start: rs5Start.format(dateFormatSeconds),
                end: rs5End.format(dateFormatSeconds),
                limit: limit.format(dateFormatSeconds),
                holes: [],
                count: 100
            };

            const expectedSlice6 = {
                start: rs6Start.format(dateFormatSeconds),
                end: limit.format(dateFormatSeconds),
                limit: limit.format(dateFormatSeconds),
                holes: [],
                count: 100
            };

            const test = await makeSlicerTest({ opConfig, recoveryData, numOfSlicers: 1 });

            const [results] = await test.createSlices();

            expect(moment(results?.start).isSame(expectedSlice1.start)).toBeTrue();
            expect(moment(results?.end).isSame(moment(expectedSlice1.end))).toBeTrue();
            expect(moment(results?.limit).isSame(moment(expectedSlice1.limit))).toBeTrue();
            expect(
                moment(results?.holes[0].start).isSame(moment(expectedSlice1.holes[0].start))
            ).toBeTrue();
            expect(
                moment(results?.holes[0].end).isSame(moment(expectedSlice1.holes[0].end))
            ).toBeTrue();

            const [results2] = await test.createSlices();

            expect(moment(results2?.start).isSame(expectedSlice2.start)).toBeTrue();
            expect(moment(results2?.end).isSame(moment(expectedSlice2.end))).toBeTrue();
            expect(moment(results2?.limit).isSame(moment(expectedSlice2.limit))).toBeTrue();
            expect(
                moment(results2?.holes[0].start).isSame(moment(expectedSlice2.holes[0].start))
            ).toBeTrue();
            expect(
                moment(results2?.holes[0].end).isSame(moment(expectedSlice2.holes[0].end))
            ).toBeTrue();

            const [results3] = await test.createSlices();
            expect(results3).toEqual(expectedSlice3);

            const [results4] = await test.createSlices();
            expect(results4).toEqual(expectedSlice4);

            const [results5] = await test.createSlices();
            expect(results5).toEqual(expectedSlice5);

            const [results6] = await test.createSlices();
            expect(results6).toEqual(expectedSlice6);

            const [results7] = await test.createSlices();
            expect(results7).toEqual(null);
        });

        it('slicer can enter recovery and return to the last slice state in persistent mode with slicer changes (1 => 2)', async () => {
            const delay: [number, moment.unitOfTime.Base] = [30, 's'];
            const currentDate = makeDate(dateFormatSeconds);
            const startDate = moment(currentDate).subtract(10, 'm');
            const middleDate = moment(currentDate).subtract(5, 'm');
            // end is delayed by setting
            const endingData = moment(currentDate).subtract(delay[0], delay[1]);
            const startTime = Date.now();

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                time_resolution: 's',
                size: 100,
                index: 'someindex',
                interval: '5m',
                delay: delay.join('')
            };

            const recoveryData = [
                {
                    lastSlice: {
                        start: startDate.format(dateFormatSeconds),
                        end: middleDate.format(dateFormatSeconds),
                        limit: endingData.format(dateFormatSeconds),
                        count: 2445
                    },
                    slicer_id: 0
                }
            ];

            const test = await makeSlicerTest({ opConfig, recoveryData, lifecycle: 'persistent' });

            // add the time (in seconds) took to run the tests
            const elasped = Math.round((Date.now() - startTime) / 1000);
            const expectedResult = {
                start: middleDate.add(elasped, 's').format(dateFormatSeconds),
                end: endingData.add(elasped, 's').format(dateFormatSeconds),
                limit: endingData.add(elasped, 's').format(dateFormatSeconds),
                holes: [],
                count: 100
            };

            const [results] = await test.createSlices();
            expect(results).toEqual(expectedResult);
        });
    });

    describe('fetcher', () => {
        it('fetcher can instantiate', async () => {
            expect.hasAssertions();

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                size: 50,
                index: 'someindex',
                full_response: true
            };

            const test = await makeFetcherTest(opConfig);
            expect(test).toBeDefined();
        });

        it('fetcher can return formated data', async () => {
            const firstDate = makeDate(dateFormatSeconds);
            const laterDate = moment(firstDate).add(5, 'm');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                size: 50,
                index: 'someindex'
            };

            const slice = {
                count: 100,
                start: firstDate.format(),
                end: laterDate.format()
            };

            const test = await makeFetcherTest(opConfig);
            const results = await test.runSlice(slice);
            expect(Array.isArray(results)).toEqual(true);

            const doc = results[0];
            expect(DataEntity.isDataEntity(doc)).toEqual(true);

            const metaData = doc.getMetadata();

            expect(typeof metaData._createTime).toEqual('number');
            expect(typeof metaData._processTime).toEqual('number');
            expect(typeof metaData._ingestTime).toEqual('number');
            expect(typeof metaData._eventTime).toEqual('number');

            expect(doc.getKey()).toEqual('someId');
            expect(metaData._index).toEqual('test-index');
            expect(metaData._type).toEqual('test-type');
            expect(metaData._version).toEqual(1);
        });
    });
});
