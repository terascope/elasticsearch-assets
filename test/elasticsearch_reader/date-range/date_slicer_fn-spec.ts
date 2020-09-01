import 'jest-extended';
import { EventEmitter } from 'events';
import {
    newTestExecutionConfig,
    LifeCycle,
    AnyObject,
    debugLogger,
    newTestJobConfig,
    times,
    pDelay,
} from '@terascope/job-components';
import moment from 'moment';
import WindowState from '../../../asset/src/elasticsearch_reader/window-state';
import slicerFn from '../../../asset/src/elasticsearch_reader/elasticsearch_date_range/slicer-fn';
import {
    SlicerArgs,
    ParsedInterval,
    SlicerDateConfig,
    DateSegments,
    SlicerDateResults
} from '../../../asset/src/elasticsearch_reader/interfaces';
import MockClient from '../../helpers/mock_client';
import { dateFormatSeconds, dateFormat, divideRange } from '../../../asset/src/elasticsearch_reader/elasticsearch_date_range/helpers';
import Reader from '../../../asset/src/elasticsearch_reader_api/reader';

interface TestConfig {
    slicers?: number;
    lifecycle?: LifeCycle;
    id?: number;
    config?: AnyObject;
    client?: MockClient;
    interval: ParsedInterval;
    latencyInterval?: ParsedInterval;
    dates: SlicerDateConfig;
    primaryRange?: DateSegments;
    timeResolution?: string;
    windowState?: WindowState;
}

describe('date slicer function', () => {
    const logger = debugLogger('dateSlicerFn');
    let events: EventEmitter;

    beforeEach(() => {
        events = new EventEmitter();
    });

    function makeSlicer({
        slicers = 1,
        lifecycle = 'once',
        id = 0,
        client: _client,
        interval,
        latencyInterval,
        dates,
        primaryRange,
        config,
        timeResolution = 's',
        windowState
    }: TestConfig) {
        let client = _client;

        if (client == null) {
            client = new MockClient();
            client.setSequenceData(times(50, () => ({ count: 100, '@timestamp': new Date() })));
        }

        if (lifecycle === 'persistent') {
            if (!primaryRange || !latencyInterval) throw new Error('Invalid test config');
        }
        const readerConfig = {
            time_resolution: timeResolution,
            size: 1000,
        };
        const job = newTestJobConfig({
            analytics: true,
            slicers,
            lifecycle,
            operations: [
                Object.assign(readerConfig, config, { _op: 'elasticsearch_reader', index: 'some_index' }),
                {
                    _op: 'noop'
                }
            ],
        });

        const executionConfig = newTestExecutionConfig(job);
        const opConfig = executionConfig.operations[0];
        const api = new Reader(opConfig as any, client, logger);
        const _windowState = windowState !== undefined ? windowState : new WindowState(slicers);

        const slicerArgs: SlicerArgs = {
            events,
            executionConfig,
            logger,
            opConfig,
            api,
            id,
            dates,
            primaryRange,
            interval,
            latencyInterval,
            windowState: _windowState
        };

        return slicerFn(slicerArgs);
    }

    function makeDate(format: string) {
        return moment.utc(moment.utc().format(format));
    }

    it('returns a function', () => {
        const interval: ParsedInterval = [5, 'm'];
        const start = makeDate(dateFormatSeconds);
        const end = moment.utc(start).add(2, 'm');
        const limit = moment.utc(start).add(interval[0], interval[1]);

        const testConfig: TestConfig = {
            interval,
            dates: {
                start,
                end,
                limit
            }
        };
        const fn = makeSlicer(testConfig);

        expect(fn).toBeFunction();
    });

    describe('can generate dates in once mode', () => {
        it('with zero count at end of slice it expands', async () => {
            const interval: ParsedInterval = [5, 'm'];
            const start = makeDate(dateFormatSeconds);
            const end = moment.utc(start).add(2, 'm');
            const limit = moment.utc(start).add(interval[0], interval[1]);

            const client = new MockClient([{ count: 0 }], 0);

            const testConfig: TestConfig = {
                interval,
                dates: {
                    start,
                    end,
                    limit
                },
                client
            };
            const expectedResults = {
                start: moment(moment.utc(start).format(dateFormatSeconds)).toISOString(),
                end: moment(moment.utc(limit).format(dateFormatSeconds)).toISOString(),
                limit: moment(moment.utc(limit).format(dateFormatSeconds)).toISOString(),
                count: 0,
                holes: []
            };

            const slicer = makeSlicer(testConfig);
            const results = await slicer();

            expect(results).toEqual(expectedResults);
        });

        it('with zero count, then to big a count at end of slice', async () => {
            const interval: ParsedInterval = [5, 'm'];
            const start = makeDate(dateFormatSeconds);
            const end = moment.utc(start).add(2, 'm');
            const limit = moment.utc(start).add(3, 'm');
            const client = new MockClient([], 2000);
            client.setSequenceData([{ count: 0 }]);

            const testConfig: TestConfig = {
                interval,
                dates: {
                    start,
                    end,
                    limit
                },
                client
            };
            const expectedResults = {
                start: moment(moment.utc(start).format(dateFormatSeconds)).toISOString(),
                end: moment(moment.utc(end).add(1, 's').format(dateFormatSeconds)).toISOString(),
                limit: moment(moment.utc(limit).format(dateFormatSeconds)).toISOString(),
                count: 2000,
                holes: []
            };

            const slicer = makeSlicer(testConfig);
            const results = await slicer();

            expect(results).toEqual(expectedResults);
        });

        it('with expanded slice count, then to large a size', async () => {
            const interval: ParsedInterval = [5, 'm'];
            const start = makeDate(dateFormatSeconds);
            const end = moment.utc(start).add(2, 'm');
            const limit = moment.utc(start).add(interval[0], interval[1]);

            const client = new MockClient([{ count: 2000 }], 2000);

            const testConfig: TestConfig = {
                interval,
                dates: {
                    start,
                    end,
                    limit
                },
                client
            };
            const expectedResults = {
                start: moment(moment.utc(start).format(dateFormatSeconds)).toISOString(),
                end: moment(moment.utc(start).add(1, 's').format(dateFormatSeconds)).toISOString(),
                limit: moment(moment.utc(limit).format(dateFormatSeconds)).toISOString(),
                count: 2000,
                holes: []
            };

            const slicer = makeSlicer(testConfig);
            const results = await slicer();

            expect(results).toEqual(expectedResults);
        });
    });

    describe('can run persistently', () => {
        it('with one slicer', async () => {
            const interval: ParsedInterval = [500, 'ms'];
            const latencyInterval: ParsedInterval = [500, 'ms'];

            const currentTime = makeDate(dateFormat);

            const limit = moment.utc(currentTime).subtract(latencyInterval[0], latencyInterval[1]);
            const start = moment.utc(limit).subtract(interval[0], interval[1]);
            const end = moment.utc(start).add(interval[0], interval[1]);

            const dates = { start, end, limit };

            const secondStart = moment.utc(limit);
            const secondEnd = moment.utc(secondStart).add(interval[0], interval[1]);
            const secondLimit = moment.utc(secondStart).add(interval[0], interval[1]);

            const thirdStart = moment.utc(secondLimit);
            const thirdEnd = moment.utc(thirdStart).add(interval[0], interval[1]);
            const thirdLimit = moment.utc(thirdStart).add(interval[0], interval[1]);

            const testConfig: TestConfig = {
                interval,
                latencyInterval,
                lifecycle: 'persistent',
                primaryRange: { start, limit },
                dates,
                timeResolution: 'ms'
            };

            const slicer = makeSlicer(testConfig);

            const results = await slicer() as SlicerDateResults;

            expect(results).toBeDefined();
            expect(moment.utc(results.start).isSame(start)).toBeTrue();
            expect(moment.utc(results.end).isSame(moment.utc(limit))).toBeTrue();
            expect(moment.utc(results.limit).isSame(moment.utc(limit))).toBeTrue();

            const results2 = await slicer();
            expect(results2).toEqual(null);

            await pDelay(500);

            const results4 = await slicer() as SlicerDateResults;

            expect(results4).toBeDefined();
            expect(moment.utc(results4.start).isSame(secondStart)).toBeTrue();
            expect(moment.utc(results4.end).isSame(secondEnd)).toBeTrue();
            expect(moment.utc(results4.limit).isSame(secondLimit)).toBeTrue();

            const results5 = await slicer();
            expect(results5).toEqual(null);

            await pDelay(500);

            const results6 = await slicer() as SlicerDateResults;

            expect(results6).toBeDefined();
            expect(moment.utc(results6.start).isSame(thirdStart)).toBeTrue();
            expect(moment.utc(results6.end).isSame(thirdEnd)).toBeTrue();
            expect(moment.utc(results6.limit).isSame(thirdLimit)).toBeTrue();
        });

        it('with one slicer with zero records returned in client then with size to large', async () => {
            const client = new MockClient();
            const zeroCount = times(1, () => ({ count: 0 }));
            const largeCount = times(50, () => ({ count: 5000 }));
            const sequence: any[] = [];

            while (zeroCount.length) {
                sequence.push(zeroCount.pop(), largeCount.pop());
            }

            client.setSequenceData(sequence.filter(Boolean));

            const interval: ParsedInterval = [500, 'ms'];
            const latencyInterval: ParsedInterval = [500, 'ms'];

            const currentTime = makeDate(dateFormat);

            const limit = moment.utc(currentTime).subtract(latencyInterval[0], latencyInterval[1]);
            const start = moment.utc(limit).subtract(interval[0], interval[1]);
            const end = moment.utc(start).add(interval[0], interval[1]);

            const dates = { start, end, limit };

            const testConfig: TestConfig = {
                interval,
                latencyInterval,
                lifecycle: 'persistent',
                primaryRange: { start, limit },
                dates,
                timeResolution: 'ms',
                client
            };

            const slicer = makeSlicer(testConfig);

            const results = await slicer() as SlicerDateResults;

            expect(results).toBeDefined();
            expect(moment.utc(results.start)).toBeDefined();
            expect(moment.utc(results.end).isSame(moment.utc(limit))).toBeDefined();
            expect(moment.utc(results.limit).isSame(moment.utc(limit))).toBeDefined();

            const results2 = await slicer();

            expect(results2).toEqual(null);
            // we test this to show that it is null not because the mock client ran out
            expect(client.sequence.length > 0).toBeTrue();
        });

        it('with multiple slicers', async () => {
            const numOfSlicer = 2;
            const interval: ParsedInterval = [500, 'ms'];
            const latencyInterval: ParsedInterval = [500, 'ms'];
            const half: ParsedInterval = [250, 'ms'];

            const currentTime = makeDate(dateFormat);

            const limit = moment.utc(currentTime).subtract(latencyInterval[0], latencyInterval[1]);
            const start = moment.utc(limit).subtract(interval[0], interval[1]);

            const primaryRange = { start, limit };

            const ranges = divideRange(start, limit, numOfSlicer);

            const secondRanges = divideRange(
                moment.utc(start).add(interval[0], interval[1]),
                moment.utc(limit).add(interval[0], interval[1]),
                numOfSlicer
            );

            const date1 = Object.assign(
                {}, ranges[0], { end: moment.utc(ranges[0].start).add(half[0], half[1]) }
            );
            const date2 = Object.assign(
                {}, ranges[1], { end: moment.utc(ranges[1].start).add(half[0], half[1]) }
            );

            const windowState = new WindowState(numOfSlicer);

            const date3 = Object.assign(
                {},
                secondRanges[0],
                { end: moment.utc(secondRanges[0].start).add(half[0], half[1]) }
            );
            const date4 = Object.assign(
                {},
                secondRanges[1],
                { end: moment.utc(secondRanges[1].start).add(half[0], half[1]) }
            );

            const testConfig1: TestConfig = {
                interval,
                latencyInterval,
                lifecycle: 'persistent',
                primaryRange,
                dates: date1,
                timeResolution: 'ms',
                slicers: numOfSlicer,
                id: 0,
                windowState
            };

            const testConfig2: TestConfig = {
                interval,
                latencyInterval,
                lifecycle: 'persistent',
                primaryRange: {
                    start: moment.utc(primaryRange.start),
                    limit: moment.utc(primaryRange.limit)
                },
                dates: date2,
                timeResolution: 'ms',
                slicers: numOfSlicer,
                id: 1,
                windowState
            };

            const slicer1 = makeSlicer(testConfig1);
            const slicer2 = makeSlicer(testConfig2);

            const results = await slicer1() as SlicerDateResults;

            expect(results).toBeDefined();
            expect(moment.utc(results.start).isSame(date1.start)).toBeTrue();
            expect(moment.utc(results.end).isSame(moment.utc(date1.limit))).toBeTrue();
            expect(moment.utc(results.limit).isSame(moment.utc(date1.limit))).toBeTrue();

            const results2 = await slicer2() as SlicerDateResults;

            expect(results2).toBeDefined();
            expect(moment.utc(results2.start).isSame(date2.start)).toBeTrue();
            expect(moment.utc(results2.end).isSame(moment.utc(date2.limit))).toBeTrue();
            expect(moment.utc(results2.limit).isSame(moment.utc(date2.limit))).toBeTrue();

            // slicer 1 is all done
            const results3 = await slicer1();
            expect(results3).toEqual(null);

            // slicer2 is all done now
            const results5 = await slicer2();
            expect(results5).toEqual(null);

            await pDelay(500);

            const results6 = await slicer1() as SlicerDateResults;

            expect(results6).toBeDefined();
            expect(moment.utc(results6.start).isSame(date3.start)).toBeTrue();
            expect(moment.utc(results6.end).isSame(moment.utc(date3.limit))).toBeTrue();
            expect(moment.utc(results6.limit).isSame(moment.utc(date3.limit))).toBeTrue();

            // slicer 1 is all done
            const results7 = await slicer1();
            expect(results7).toEqual(null);

            await pDelay(500);

            // slicer 1 is still all done because slicer2 has not sliced yet
            const results8 = await slicer1();
            expect(results8).toEqual(null);

            const results9 = await slicer2() as SlicerDateResults;

            expect(results9).toBeDefined();
            expect(moment.utc(results9.start).isSame(date4.start)).toBeTrue();
            expect(moment.utc(results9.end).isSame(moment.utc(date4.limit))).toBeTrue();
            expect(moment.utc(results9.limit).isSame(moment.utc(date4.limit))).toBeTrue();
        });
    });
});
