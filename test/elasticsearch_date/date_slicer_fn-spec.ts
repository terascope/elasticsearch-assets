import EventEmitter from 'events';
import {
    getTypeOf,
    newTestExecutionConfig,
    LifeCycle,
    AnyObject,
    debugLogger,
    newTestJobConfig,
    times
} from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import moment from 'moment';
import WindowState from '../../asset/src/elasticsearch_reader/window-state';
import slicerFn from '../../asset/src/elasticsearch_reader/elasticsearch_date_range/slicer';
import {
    SlicerArgs,
    ParsedInterval,
    SlicerDateConfig,
    DateSegments
} from '../../asset/src/elasticsearch_reader/interfaces';
import MockClient from '../mock_client';

interface TestConfig {
    slicers?: number;
    lifecycle?: LifeCycle;
    id?: number;
    config?: AnyObject;
    client?: any;
    interval: ParsedInterval;
    latencyInterval?: ParsedInterval;
    dates: SlicerDateConfig;
    primaryRange?: DateSegments;
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
        client = new MockClient(),
        interval,
        latencyInterval,
        dates,
        primaryRange,
        config
    }: TestConfig) {
        if (lifecycle === 'persistent') {
            if (!primaryRange || !latencyInterval) throw new Error('Invalid test config');
        }
        const readerConfig = {
            time_resolution: 's',
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

        client.setSequenceData(times(50, () => ({ count: 100, '@timestamp': new Date() })));

        const executionConfig = newTestExecutionConfig(job);
        const opConfig = executionConfig.operations[0];
        // @ts-ignore
        const api = elasticApi(client, logger, opConfig);
        const windowState = new WindowState(slicers);

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
            windowState
        };

        return slicerFn(slicerArgs);
    }

    it('returns a function', () => {
        const interval: ParsedInterval = [5, 'm'];
        const start = moment();
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

        expect(getTypeOf(fn)).toEqual('Function');
    });

    fit('can run a persistently', async () => {
        const interval: ParsedInterval = [5, 'm'];

        const limit = moment();
        const start = moment(limit).subtract(7, 'm');
        const end = moment(start).add(interval[0], interval[1]);

        console.log({ start, end, limit });

        const dates = { start, end, limit };

        const testConfig: TestConfig = {
            interval,
            latencyInterval: [1000, 'ms'],
            dates
        };

        const slicer = makeSlicer(testConfig);

        const results = await slicer({});
        console.log('results', results);

        const results2 = await slicer({});
        console.log('results2', results2);

        const results3 = await slicer({});
        console.log('results3', results3);
    });
});
