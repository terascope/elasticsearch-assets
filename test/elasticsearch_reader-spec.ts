/* eslint-disable @typescript-eslint/no-unused-vars */

import {
    TestContext, DataEntity, startsWith, get, pDelay
} from '@terascope/job-components';
import path from 'path';
import moment from 'moment';
import { WorkerTestHarness, SlicerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import MockClient from './mock_client';
import Schema from '../asset/src/elasticsearch_reader/schema';
import { IDType } from '../asset/src/id_reader/interfaces';

describe('elasticsearch_reader', () => {
    const assetDir = path.join(__dirname, '..');
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
            },
            {
                type: 'elasticsearch',
                endpoint: 'otherConnection',
                create: () => ({
                    client: new MockClient()
                }),
            }
        ];
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    async function makeFetcherTest(config: any) {
        harness = WorkerTestHarness.testFetcher(config, { assetDir, clients });
        await harness.initialize();
        return harness;
    }

    interface EventHook {
        event: string;
        fn: (event?: any) => void;
    }

    async function makeSlicerTest(
        config: any, numOfSlicers = 1,
        recoveryData?: object[],
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
                type: 'some-type',
                date_field_name: 'created'
            };
            const test = await makeFetcherTest(testOpConfig);
            const {
                opConfig: {
                    connection,
                    size,
                    interval,
                    preserve_id: preserveId,
                    delay,
                    subslice_by_key: subslice,
                    time_resolution: resolution
                }
            } = test.getOperation(opName);

            expect(connection).toEqual('default');
            expect(size).toEqual(5000);
            expect(interval).toEqual('auto');
            expect(preserveId).toEqual(false);
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
            // @ts-ignore
            expect(() => geoPointValidation(19.1234)).toThrowError('parameter must be a string IF specified');
            expect(() => geoPointValidation('19.1234')).toThrowError('Invalid geo_point, received 19.1234');
            expect(() => geoPointValidation('190.1234,85.2134')).toThrowError('latitude parameter is incorrect, was given 190.1234, should be >= -90 and <= 90');
            expect(() => geoPointValidation('80.1234,185.2134')).toThrowError('longitutde parameter is incorrect, was given 185.2134, should be >= -180 and <= 180');
            expect(() => geoPointValidation('80.1234,-155.2134')).not.toThrowError();
            // @ts-ignore
            expect(() => validGeoDistance(19.1234)).toThrowError('parameter must be a string IF specified');
            expect(() => validGeoDistance(' ')).toThrowError('geo_distance paramter is formatted incorrectly');
            expect(() => validGeoDistance('200something')).toThrowError('unit type did not have a proper unit of measuerment (ie m, km, yd, ft)');
            expect(() => validGeoDistance('200km')).not.toThrowError();

            expect(() => geoSortOrder(1234)).toThrowError('parameter must be a string IF specified');
            expect(() => geoSortOrder('hello')).toThrowError('if geo_sort_order is specified it must be either "asc" or "desc"');
            expect(() => geoSortOrder('asc')).not.toThrowError();
        });

        it('subslice_by_key configuration validation', async () => {
            expect.hasAssertions();
            const errorString = 'If subslice_by_key is set to true, the elasticsearch type parameter of the documents must also be set';
            const badOP = { subslice_by_key: true };
            const goodOP = { subslice_by_key: true, type: 'events-' };
            const otherGoodOP = { subslice_by_key: false, type: 'events-' };
            // NOTE: geo self validations are tested in elasticsearch_api module

            const testOpConfig = {
                _op: 'elasticsearch_reader',
                index: 'some-index',
                type: 'some-type',
                date_field_name: 'created'
            };

            try {
                await makeFetcherTest(Object.assign({}, testOpConfig, badOP));
            } catch (err) {
                expect(err.message).toEqual(errorString);
            }

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

            const test = await makeSlicerTest(opConfig);
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

            const test = await makeSlicerTest(opConfig, 2);
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
            const errMsg = 'date_field_name: "date" for index: someindex does not exist';

            try {
                await makeSlicerTest(opConfig);
            } catch (err) {
                expect(startsWith(err.message, errMsg)).toEqual(true);
            }
        });

        it('slicers will emit updated operations for start and end', async () => {
            const firstDate = moment();
            const laterDate = moment(firstDate).add(5, 'm');
            let updatedConfig: any;

            function checkUpdate(updateObj: any) {
                updatedConfig = get(updateObj, 'update[0]');
                return true;
            }

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
                const test = await makeSlicerTest(config, 1, [], { event: 'slicer:execution:update', fn: checkUpdate });
                await pDelay(30);
                return test;
            }

            const test1 = await waitForUpdate(opConfig, firstDate);
            expect(updatedConfig.start).toEqual(firstDate.format());
            expect(updatedConfig.end).toEqual(moment(firstDate).add(1, 's').format());
            await test1.shutdown();

            const test2 = await waitForUpdate(opConfig2);
            expect(updatedConfig.start).toEqual(firstDate.format());
            expect(updatedConfig.end).toEqual(moment(firstDate).add(5, 'm').add(1, 's').format());
            await test2.shutdown();

            const test3 = await waitForUpdate(opConfig3);
            expect(updatedConfig.start).toEqual(firstDate.format());
            expect(updatedConfig.end).toEqual(moment(firstDate).add(5, 'm').add(1, 's').format());
            await test3.shutdown();

            const test4 = await waitForUpdate(opConfig4);
            expect(updatedConfig.start).toEqual(firstDate.format());
            expect(updatedConfig.end).toEqual(moment(firstDate).add(5, 'm').add(1, 's').format());
            await test4.shutdown();
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
            const test = await makeSlicerTest(opConfig);
            const results = await test.createSlices();

            expect(results).toEqual([null]);
        });

        it('slicer can produce date slices', async () => {
            const firstDate = moment();
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

            const test = await makeSlicerTest(opConfig);
            const [results] = await test.createSlices();

            expect(results.start).toEqual(firstDate.format());
            expect(results.end).toEqual(closingDate.format());
            expect(results.count).toEqual(100);

            const [results2] = await test.createSlices();
            expect(results2).toEqual(null);
        });

        it('slicer can reduce date slices down to size', async () => {
            const firstDate = moment();
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

            const test = await makeSlicerTest(opConfig, 1, [], { event: 'slicer:slice:recursion', fn: hasRecursedEvent });

            const [results] = await test.createSlices();

            expect(results.start).toEqual(firstDate.format());
            expect(results.end).toEqual(middleDate.format());
            expect(results.count).toEqual(50);

            const [results2] = await test.createSlices();

            expect(hasRecursed).toEqual(true);
            expect(results2.start).toEqual(middleDate.format());
            expect(results2.end).toEqual(closingDate.format());
            expect(results2.count).toEqual(50);

            const results3 = await test.createSlices();
            expect(results3).toEqual([null]);
        });

        it('slicer can do a simple expansion of date slices up to find data', async () => {
            const firstDate = moment();
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

            const test = await makeSlicerTest(opConfig, 1, [], { event: 'slicer:slice:range_expansion', fn: hasExpandedFn });
            const [results] = await test.createSlices();

            expect(results.start).toEqual(firstDate.format());
            expect(results.end).toEqual(endDate.format());
            expect(results.count).toEqual(100);

            const [results2] = await test.createSlices();

            expect(hasExpanded).toEqual(true);
            expect(results2.start).toEqual(endDate.format());
            expect(results2.end).toEqual(closingDate.format());
            expect(results2.count).toEqual(100);

            const results3 = await test.createSlices();
            expect(results3).toEqual([null]);
        });


        it('slicer can do an expansion of date slices up to find data even when none is returned', async () => {
            const firstDate = moment();
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

            const test = await makeSlicerTest(opConfig, 1, [], { event: 'slicer:slice:range_expansion', fn: hasExpandedFn });
            const [results] = await test.createSlices();

            expect(results.start).toEqual(firstDate.format());
            expect(results.end).toEqual(closingDate.format());
            expect(results.count).toEqual(0);

            expect(hasExpanded).toEqual(true);

            const results2 = await test.createSlices();
            expect(results2).toEqual([null]);
        });

        it('slicer can do expansion of date slices with large slices', async () => {
            const firstDate = moment();
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

            const test = await makeSlicerTest(opConfig, 1, [], { event: 'slicer:slice:range_expansion', fn: hasExpandedFn });
            const [results] = await test.createSlices();

            expect(results.start).toEqual(firstDate.format());
            expect(moment(results.end).isBetween(middleDate, endDate)).toEqual(true);
            expect(results.count).toEqual(100);

            const [results2] = await test.createSlices();
            expect(hasExpanded).toEqual(true);
            expect(moment(results2.start).isBetween(middleDate, endDate)).toEqual(true);
            expect(results2.end).toEqual(closingDate.format());
            expect(results2.count).toEqual(100);

            const results3 = await test.createSlices();
            expect(results3).toEqual([null]);
        });

        it('slicer can expand date slices properly in uneven data distribution', async () => {
            const firstDate = moment();
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

            const test = await makeSlicerTest(opConfig, 1, [], { event: 'slicer:slice:range_expansion', fn: hasExpandedFn });
            const [results] = await test.createSlices();

            expect(results.start).toEqual(firstDate.format());
            expect(moment(results.end).isBetween(firstDate, midDate)).toEqual(true);
            expect(results.count).toEqual(100);

            const [results2] = await test.createSlices();

            expect(moment(results2.end).isBetween(midDate, endDate)).toEqual(true);
            expect(hasExpanded).toEqual(true);
            expect(results2.count).toEqual(100);

            const [results3] = await test.createSlices();

            expect(moment(results3.end).isBetween(midDate, endDate)).toEqual(true);

            const [results4] = await test.createSlices();
            expect(results4.end).toEqual(closingDate.format());

            const results5 = await test.createSlices();
            expect(results5).toEqual([null]);
        });

        it('slicer can will recurse down to smallest factor using "s" format', async () => {
            const firstDateMS = moment().toISOString();
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

            const test = await makeSlicerTest(opConfig);
            const [resultsS] = await test.createSlices();

            expect(resultsS.start).toEqual(firstDateS.format());
            expect(resultsS.end).toEqual(closingDateS.format());
            expect(resultsS.count).toEqual(100);
        });

        it('slicer can will recurse down to smallest factor using "ms" format', async () => {
            const firstDateMS = moment().toISOString();
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
            const test = await makeSlicerTest(opConfig);
            const [resultsMS] = await test.createSlices();


            const startMsIsSame = moment(resultsMS.start).isSame(moment(firstDateMS));
            const endMsIsSame = moment(resultsMS.end).isSame(moment(closingDateMS));

            expect(startMsIsSame).toEqual(true);
            expect(endMsIsSame).toEqual(true);
            expect(resultsMS.count).toEqual(100);
        });

        it('slicer can will recurse down to smallest factor and subslice by key', async () => {
            const firstDate = moment();
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
                type: 'test'
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

            const test = await makeSlicerTest(opConfig);
            const results = await test.createSlices();

            hexadecimal.forEach((char) => {
                const subslice = results.find((s) => s.key === `test#${char}*`);
                expect(subslice).not.toBeUndefined();
                expect(subslice!.start!.format() === firstDate.format()).toEqual(true);
                expect(subslice!.end!.format() === closingDate.format()).toEqual(true);
            });
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

        it('newReader can return formated data', async () => {
            const firstDate = moment();
            const laterDate = moment(firstDate).add(5, 'm');

            const opConfig = {
                _op: 'elasticsearch_reader',
                date_field_name: '@timestamp',
                size: 50,
                index: 'someindex'
            };
            const slice = { count: 100, start: firstDate.format(), end: laterDate.format() };

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
