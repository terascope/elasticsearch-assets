import moment from 'moment';
import { determineStartingPoint } from '../../asset/src/__lib';
import { StartPointConfig, ParsedInterval } from '../../asset/src/elasticsearch_reader/interfaces';

describe('determination of starting point function', () => {
    it('can return point and range when no recovery is given for a single slicer', () => {
        const interval: ParsedInterval = [1, 'm'];
        const limit = moment();
        const start = moment(limit).subtract(2, 'm');
        const end = moment(start).add(interval[0], interval[1]);

        const config: StartPointConfig = {
            id: 0,
            numOfSlicers: 1,
            dates: { start, limit },
            interval,
            recoveryData: []
        };

        const { dates, range } = determineStartingPoint(config);

        expect(dates).toEqual({ start, end, limit });
        expect(range).toEqual(config.dates);
    });

    it('can return point and range when no recovery is given for two slicers', () => {
        const interval: ParsedInterval = [1, 'm'];
        const endLimit = moment();
        const startOfRange = moment(endLimit).subtract(2, 'm');
        const firstSegmentLimit = moment(startOfRange).add(interval[0], interval[1]);

        const config1: StartPointConfig = {
            id: 0,
            numOfSlicers: 2,
            dates: { start: startOfRange, limit: endLimit },
            interval,
            recoveryData: []
        };

        const config2: StartPointConfig = {
            id: 1,
            numOfSlicers: 2,
            dates: { start: startOfRange, limit: endLimit },
            interval,
            recoveryData: []
        };

        const { dates: dates1, range: range1 } = determineStartingPoint(config1);

        expect(dates1).toEqual({
            start: startOfRange,
            end: firstSegmentLimit,
            limit: firstSegmentLimit
        });
        expect(range1).toEqual(config1.dates);

        const { dates: dates2, range: range2 } = determineStartingPoint(config2);

        expect(dates2).toEqual({
            start: firstSegmentLimit,
            end: endLimit,
            limit: endLimit
        });
        expect(range2).toEqual(config2.dates);
    });

    it('can return point and range when recovery (1 point, no holes) is given for single slicer', () => {
        const interval: ParsedInterval = [1, 'm'];
        const limit = moment();
        const start = moment(limit).subtract(2, 'm');
        const end = moment(start).add(interval[0], interval[1]);

        const config: StartPointConfig = {
            id: 0,
            numOfSlicers: 1,
            dates: { start, limit },
            interval,
            recoveryData: [{

            }]
        };

        const { dates, range } = determineStartingPoint(config);

        expect(dates).toEqual({ start, end, limit });
        expect(range).toEqual(config.dates);
    });
});
