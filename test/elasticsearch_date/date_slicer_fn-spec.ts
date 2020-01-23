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
import elasticApi from '@terascope/elasticsearch-api';
import moment from 'moment';
import WindowState from '../../asset/src/elasticsearch_reader/window-state';
import slicerFn from '../../asset/src/elasticsearch_reader/elasticsearch_date_range/slicer-fn';
import {
    SlicerArgs,
    ParsedInterval,
    SlicerDateConfig,
    DateSegments,
    SlicerDateResults
} from '../../asset/src/elasticsearch_reader/interfaces';
import MockClient from '../mock_client';
import { dateFormatSeconds, dateFormat, divideRange } from '../../asset/src/elasticsearch_reader/elasticsearch_date_range/helpers';

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
        const api = elasticApi(client, logger, opConfig as any);
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
        return moment(moment().format(format));
    }

    it('returns a function', () => {
        const interval: ParsedInterval = [5, 'm'];
        const start = makeDate(dateFormatSeconds);
        const end = moment(start).add(2, 'm');
        const limit = moment(start).add(interval[0], interval[1]);

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

    it('can run a persistently with one slicer', async () => {
        const interval: ParsedInterval = [500, 'ms'];
        const latencyInterval: ParsedInterval = [500, 'ms'];

        const currentTime = makeDate(dateFormat);

        const limit = moment(currentTime).subtract(latencyInterval[0], latencyInterval[1]);
        const start = moment(limit).subtract(interval[0], interval[1]);
        const end = moment(start).add(interval[0], interval[1]);

        const dates = { start, end, limit };

        const secondStart = moment(limit);
        const secondEnd = moment(secondStart).add(interval[0], interval[1]);
        const secondLimit = moment(secondStart).add(interval[0], interval[1]);

        const thirdStart = moment(secondLimit);
        const thirdEnd = moment(thirdStart).add(interval[0], interval[1]);
        const thirdLimit = moment(thirdStart).add(interval[0], interval[1]);

        const testConfig: TestConfig = {
            interval,
            latencyInterval,
            lifecycle: 'persistent',
            primaryRange: { start, limit },
            dates,
            timeResolution: 'ms'
        };

        const slicer = makeSlicer(testConfig);

        const results = await slicer({}) as SlicerDateResults;

        expect(results).toBeDefined();
        expect(moment(results.start).isSame(start)).toBeTrue();
        expect(moment(results.end).isSame(moment(limit))).toBeTrue();
        expect(moment(results.limit).isSame(moment(limit))).toBeTrue();

        const results2 = await slicer({});
        expect(results2).toEqual(null);

        await pDelay(500);

        const results4 = await slicer({}) as SlicerDateResults;

        expect(results4).toBeDefined();
        expect(moment(results4.start).isSame(secondStart)).toBeTrue();
        expect(moment(results4.end).isSame(secondEnd)).toBeTrue();
        expect(moment(results4.limit).isSame(secondLimit)).toBeTrue();

        const results5 = await slicer({});
        expect(results5).toEqual(null);

        await pDelay(500);

        const results6 = await slicer({}) as SlicerDateResults;

        expect(results6).toBeDefined();
        expect(moment(results6.start).isSame(thirdStart)).toBeTrue();
        expect(moment(results6.end).isSame(thirdEnd)).toBeTrue();
        expect(moment(results6.limit).isSame(thirdLimit)).toBeTrue();
    });

    it('can run a persistently with one slicer with zero records returned in client then with size to large', async () => {
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

        const limit = moment(currentTime).subtract(latencyInterval[0], latencyInterval[1]);
        const start = moment(limit).subtract(interval[0], interval[1]);
        const end = moment(start).add(interval[0], interval[1]);

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

        const results = await slicer({}) as SlicerDateResults;

        expect(results).toBeDefined();
        expect(moment(results.start)).toBeDefined();
        expect(moment(results.end).isSame(moment(limit))).toBeDefined();
        expect(moment(results.limit).isSame(moment(limit))).toBeDefined();

        const results2 = await slicer({});

        expect(results2).toEqual(null);
        // we test this to show that it is null not becuase the mock client ran out
        expect(client.sequence.length > 0).toBeTrue();
    });

    it('can run persistenly with multiple slicers', async () => {
        const numOfSlicer = 2;
        const interval: ParsedInterval = [500, 'ms'];
        const latencyInterval: ParsedInterval = [500, 'ms'];
        const half: ParsedInterval = [250, 'ms'];

        const currentTime = makeDate(dateFormat);

        const limit = moment(currentTime).subtract(latencyInterval[0], latencyInterval[1]);
        const start = moment(limit).subtract(interval[0], interval[1]);

        const primaryRange = { start, limit };

        const ranges = divideRange(start, limit, numOfSlicer);

        const secondRanges = divideRange(
            moment(start).add(interval[0], interval[1]),
            moment(limit).add(interval[0], interval[1]),
            numOfSlicer
        );

        const date1 = Object.assign(
            {}, ranges[0], { end: moment(ranges[0].start).add(half[0], half[1]) }
        );
        const date2 = Object.assign(
            {}, ranges[1], { end: moment(ranges[1].start).add(half[0], half[1]) }
        );

        const windowState = new WindowState(numOfSlicer);

        const date3 = Object.assign(
            {}, secondRanges[0], { end: moment(secondRanges[0].start).add(half[0], half[1]) }
        );
        const date4 = Object.assign(
            {}, secondRanges[1], { end: moment(secondRanges[1].start).add(half[0], half[1]) }
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
            primaryRange: { start: moment(primaryRange.start), limit: moment(primaryRange.limit) },
            dates: date2,
            timeResolution: 'ms',
            slicers: numOfSlicer,
            id: 1,
            windowState
        };

        const slicer1 = makeSlicer(testConfig1);
        const slicer2 = makeSlicer(testConfig2);

        const results = await slicer1({}) as SlicerDateResults;

        expect(results).toBeDefined();
        expect(moment(results.start).isSame(date1.start)).toBeTrue();
        expect(moment(results.end).isSame(moment(date1.limit))).toBeTrue();
        expect(moment(results.limit).isSame(moment(date1.limit))).toBeTrue();

        const results2 = await slicer2({}) as SlicerDateResults;

        expect(results2).toBeDefined();
        expect(moment(results2.start).isSame(date2.start)).toBeTrue();
        expect(moment(results2.end).isSame(moment(date2.limit))).toBeTrue();
        expect(moment(results2.limit).isSame(moment(date2.limit))).toBeTrue();

        // slicer 1 is all done
        const results3 = await slicer1({});
        expect(results3).toEqual(null);

        // slicer2 is all done now
        const results5 = await slicer2({});
        expect(results5).toEqual(null);

        await pDelay(500);

        const results6 = await slicer1({}) as SlicerDateResults;

        expect(results6).toBeDefined();
        expect(moment(results6.start).isSame(date3.start)).toBeTrue();
        expect(moment(results6.end).isSame(moment(date3.limit))).toBeTrue();
        expect(moment(results6.limit).isSame(moment(date3.limit))).toBeTrue();

        // slicer 1 is all done
        const results7 = await slicer1({});
        expect(results7).toEqual(null);

        await pDelay(500);

        // slicer 1 is still all done because slicer2 has not sliced yet
        const results8 = await slicer1({});
        expect(results8).toEqual(null);

        const results9 = await slicer2({}) as SlicerDateResults;

        expect(results9).toBeDefined();
        expect(moment(results9.start).isSame(date4.start)).toBeTrue();
        expect(moment(results9.end).isSame(moment(date4.limit))).toBeTrue();
        expect(moment(results9.limit).isSame(moment(date4.limit))).toBeTrue();
    });
});
