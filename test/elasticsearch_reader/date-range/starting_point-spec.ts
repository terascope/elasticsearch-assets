import 'jest-extended';
import moment from 'moment';
import { cloneDeep } from '@terascope/job-components';
import { determineStartingPoint, dateFormatSeconds, divideRange } from '../../../asset/src/elasticsearch_reader/elasticsearch_date_range/helpers';
import { StartPointConfig, ParsedInterval } from '../../../asset/src/elasticsearch_reader/interfaces';

function makeDate(format: string) {
    return moment.utc(moment.utc().format(format));
}

describe('determineStartingPoint', () => {
    describe('for same slicer counts', () => {
        it('can return point and range when no recovery is given for a single slicer', () => {
            const interval: ParsedInterval = [1, 'm'];
            const limit = makeDate(dateFormatSeconds);
            const start = moment.utc(limit).subtract(2, 'm');
            const end = moment.utc(start).add(interval[0], interval[1]);

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
            const endLimit = makeDate(dateFormatSeconds);
            const startOfRange = moment.utc(endLimit).subtract(2, 'm');
            const firstSegmentLimit = moment.utc(startOfRange).add(interval[0], interval[1]);

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
            const limit = makeDate(dateFormatSeconds);
            const start = moment.utc(limit).subtract(2, 'm');
            const halfWay = moment.utc(start).add(interval[0], interval[1]);
            const recoveryStart = moment.utc(halfWay).subtract(30, 's');
            const recoveryEnd = moment.utc(recoveryStart).add(interval[0], interval[1]);

            const config: StartPointConfig = {
                id: 0,
                numOfSlicers: 1,
                dates: { start, limit },
                interval,
                recoveryData: [{
                    start: recoveryStart.format(dateFormatSeconds),
                    end: recoveryEnd.format(dateFormatSeconds),
                    limit: moment.utc(limit).format(dateFormatSeconds),
                    holes: [],
                    count: 100
                }]
            };

            const { dates, range } = determineStartingPoint(config);

            expect(dates.start.isSame(moment.utc(recoveryEnd))).toBeTrue();
            expect(dates.end.isSame(moment.utc(limit))).toBeTrue();
            expect(dates.limit.isSame(moment.utc(limit))).toBeTrue();

            expect(range.start.isSame(config.dates.start)).toBeTrue();
            expect(range.limit.isSame(config.dates.limit)).toBeTrue();
        });

        it('can return point and range when recovery (1 point, with hole in middle) is given for single slicer', () => {
            const interval: ParsedInterval = [1, 'm'];
            const limit = makeDate(dateFormatSeconds);
            const start = moment.utc(limit).subtract(2, 'm');
            const halfWay = moment.utc(start).add(interval[0], interval[1]);
            const recoveryStart = moment.utc(halfWay).subtract(30, 's');
            const recoveryEnd = moment.utc(recoveryStart).add(interval[0], interval[1]);

            const holeStart = moment.utc(recoveryEnd).add(10, 's').format(dateFormatSeconds);
            const holeEnd = moment.utc(holeStart).add(10, 's').format(dateFormatSeconds);
            const holes = [{ start: holeStart, end: holeEnd }];

            const config: StartPointConfig = {
                id: 0,
                numOfSlicers: 1,
                dates: { start, limit },
                interval,
                recoveryData: [{
                    start: recoveryStart.format(dateFormatSeconds),
                    end: recoveryEnd.format(dateFormatSeconds),
                    limit: moment.utc(limit).format(dateFormatSeconds),
                    holes,
                    count: 100
                }]
            };

            const { dates, range } = determineStartingPoint(config);

            expect(dates.start.isSame(moment.utc(recoveryEnd))).toBeTrue();
            expect(dates.end.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates.limit.isSame(moment.utc(limit))).toBeTrue();
            expect(dates.holes).toEqual(holes);

            expect(range.start.isSame(config.dates.start)).toBeTrue();
            expect(range.limit.isSame(config.dates.limit)).toBeTrue();
        });

        it('can return point and range when recovery (1 point, with hole at start) is given for single slicer', () => {
            const interval: ParsedInterval = [1, 'm'];
            const limit = makeDate(dateFormatSeconds);
            const start = moment.utc(limit).subtract(2, 'm');
            const halfWay = moment.utc(start).add(interval[0], interval[1]);
            const recoveryStart = moment.utc(halfWay).subtract(30, 's');
            const recoveryEnd = moment.utc(recoveryStart).add(interval[0], interval[1]);

            const holeStart = moment.utc(recoveryEnd).format(dateFormatSeconds);
            const holeEnd = moment.utc(holeStart).add(10, 's').format(dateFormatSeconds);
            const holes = [{ start: holeStart, end: holeEnd }];

            const config: StartPointConfig = {
                id: 0,
                numOfSlicers: 1,
                dates: { start, limit },
                interval,
                recoveryData: [{
                    start: recoveryStart.format(dateFormatSeconds),
                    end: recoveryEnd.format(dateFormatSeconds),
                    limit: moment.utc(limit).format(dateFormatSeconds),
                    holes,
                    count: 100
                }]
            };

            const { dates, range } = determineStartingPoint(config);

            expect(dates.start.isSame(moment.utc(holeEnd))).toBeTrue();
            expect(dates.end.isSame(moment.utc(limit))).toBeTrue();
            expect(dates.limit.isSame(moment.utc(limit))).toBeTrue();
            expect(dates.holes).toEqual([]);

            expect(range.start.isSame(config.dates.start)).toBeTrue();
            expect(range.limit.isSame(config.dates.limit)).toBeTrue();
        });

        it('can return point and range when recovery (1 point, with hole at limit, overflows) is given for single slicer', () => {
            const interval: ParsedInterval = [1, 'm'];
            const limit = makeDate(dateFormatSeconds);
            const start = moment.utc(limit).subtract(2, 'm');
            const halfWay = moment.utc(start).add(interval[0], interval[1]);
            const recoveryStart = moment.utc(halfWay).subtract(30, 's');
            const recoveryEnd = moment.utc(recoveryStart).add(interval[0], interval[1]);

            const holeStart = moment.utc(recoveryEnd).add(10, 's').format(dateFormatSeconds);
            // hole jumps past limit
            const holeEnd = moment.utc(holeStart).add(5, 'm').format(dateFormatSeconds);
            const holes = [{ start: holeStart, end: holeEnd }];

            const config: StartPointConfig = {
                id: 0,
                numOfSlicers: 1,
                dates: { start, limit },
                interval,
                recoveryData: [{
                    start: recoveryStart.format(dateFormatSeconds),
                    end: recoveryEnd.format(dateFormatSeconds),
                    limit: moment.utc(limit).format(dateFormatSeconds),
                    holes,
                    count: 100
                }]
            };

            const { dates, range } = determineStartingPoint(config);

            expect(dates.start.isSame(moment.utc(recoveryEnd))).toBeTrue();
            expect(dates.end.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates.limit.isSame(moment.utc(holeStart))).toBeTrue();
            // we keep holes because a persistent reader might need it on next boundary increase
            expect(dates.holes).toEqual(holes);

            expect(range.start.isSame(config.dates.start)).toBeTrue();
            expect(range.limit.isSame(config.dates.limit)).toBeTrue();
        });

        it('can return point and range when recovery (1 point, with hole at limit, exact match) is given for single slicer', () => {
            const interval: ParsedInterval = [1, 'm'];
            const limit = makeDate(dateFormatSeconds);
            const start = moment.utc(limit).subtract(2, 'm');
            const halfWay = moment.utc(start).add(interval[0], interval[1]);
            const recoveryStart = moment.utc(halfWay).subtract(30, 's');
            const recoveryEnd = moment.utc(recoveryStart).add(interval[0], interval[1]);

            const holeStart = moment.utc(recoveryEnd).add(10, 's').format(dateFormatSeconds);
            // hole jumps past limit
            const holeEnd = moment.utc(limit).format(dateFormatSeconds);
            const holes = [{ start: holeStart, end: holeEnd }];

            const config: StartPointConfig = {
                id: 0,
                numOfSlicers: 1,
                dates: { start, limit },
                interval,
                recoveryData: [{
                    start: recoveryStart.format(dateFormatSeconds),
                    end: recoveryEnd.format(dateFormatSeconds),
                    limit: moment.utc(limit).format(dateFormatSeconds),
                    holes,
                    count: 100
                }]
            };

            const { dates, range } = determineStartingPoint(config);

            expect(dates.start.isSame(moment.utc(recoveryEnd))).toBeTrue();
            expect(dates.end.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates.limit.isSame(moment.utc(holeStart))).toBeTrue();
            // hole exact match limit so hole is encapsulated, so we can toss it
            expect(dates.holes).toEqual([]);

            expect(range.start.isSame(config.dates.start)).toBeTrue();
            expect(range.limit.isSame(config.dates.limit)).toBeTrue();
        });

        it('can return point and range when recovery (1 point, with hole covering whats left of slice range) is given for single slicer', () => {
            const interval: ParsedInterval = [1, 'm'];
            const limit = makeDate(dateFormatSeconds);
            const start = moment.utc(limit).subtract(2, 'm');
            const halfWay = moment.utc(start).add(interval[0], interval[1]);
            const recoveryStart = moment.utc(halfWay).subtract(30, 's');
            const recoveryEnd = moment.utc(recoveryStart).add(interval[0], interval[1]);

            const holeStart = moment.utc(recoveryEnd).format(dateFormatSeconds);
            const holeEnd = moment.utc(limit).format(dateFormatSeconds);
            const holes = [{ start: holeStart, end: holeEnd }];

            const config: StartPointConfig = {
                id: 0,
                numOfSlicers: 1,
                dates: { start, limit },
                interval,
                recoveryData: [{
                    start: recoveryStart.format(dateFormatSeconds),
                    end: recoveryEnd.format(dateFormatSeconds),
                    limit: moment.utc(limit).format(dateFormatSeconds),
                    holes,
                    count: 100
                }]
            };

            const { dates, range } = determineStartingPoint(config);

            expect(dates.start.isSame(moment.utc(recoveryEnd))).toBeTrue();
            expect(dates.end.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates.limit.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates.holes).toEqual([]);

            expect(range.start.isSame(config.dates.start)).toBeTrue();
            expect(range.limit.isSame(config.dates.limit)).toBeTrue();
        });

        it('can return point and range when recovery (1 point, with hole covering all alloted range) is given for single slicer', () => {
            const interval: ParsedInterval = [1, 'm'];
            const limit = makeDate(dateFormatSeconds);
            const start = moment.utc(limit).subtract(2, 'm');
            const halfWay = moment.utc(start).add(interval[0], interval[1]);
            const recoveryStart = moment.utc(halfWay).subtract(30, 's');
            const recoveryEnd = moment.utc(recoveryStart).add(interval[0], interval[1]);

            const holeStart = moment.utc(recoveryEnd).format(dateFormatSeconds);
            const holeEnd = moment.utc(limit)
                .add(interval[0], interval[1])
                .format(dateFormatSeconds);

            const holes = [{ start: holeStart, end: holeEnd }];

            const config: StartPointConfig = {
                id: 0,
                numOfSlicers: 1,
                dates: { start, limit },
                interval,
                recoveryData: [{
                    start: recoveryStart.format(dateFormatSeconds),
                    end: recoveryEnd.format(dateFormatSeconds),
                    limit: moment.utc(limit).format(dateFormatSeconds),
                    holes,
                    count: 100
                }]
            };

            const { dates, range } = determineStartingPoint(config);

            expect(dates.start.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates.end.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates.limit.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates.holes).toEqual(holes);

            expect(range.start.isSame(config.dates.start)).toBeTrue();
            expect(range.limit.isSame(config.dates.limit)).toBeTrue();
        });

        it('can return point and range when recovery (2 points, no holes) for two slicers', () => {
            const interval: ParsedInterval = [1, 'm'];
            const endLimit = makeDate(dateFormatSeconds);
            const startOfRange = moment.utc(endLimit).subtract(2, 'm');
            const firstSegmentLimit = moment.utc(startOfRange).add(interval[0], interval[1]);

            const recoveryStartSlicerOne = moment.utc(firstSegmentLimit).subtract(30, 's');
            const recoveryEndSlicerOne = moment.utc(recoveryStartSlicerOne).add(15, 's');

            const recoveryStartSlicerTwo = moment.utc(firstSegmentLimit).add(30, 's');
            const recoveryEndSlicerTwo = moment.utc(recoveryStartSlicerTwo).add(15, 's');

            const config1: StartPointConfig = {
                id: 0,
                numOfSlicers: 2,
                dates: { start: startOfRange, limit: endLimit },
                interval,
                recoveryData: [{
                    start: recoveryStartSlicerOne.format(dateFormatSeconds),
                    end: recoveryEndSlicerOne.format(dateFormatSeconds),
                    limit: moment.utc(firstSegmentLimit).format(dateFormatSeconds),
                    holes: [],
                    count: 100
                }, {
                    start: recoveryStartSlicerTwo.format(dateFormatSeconds),
                    end: recoveryEndSlicerTwo.format(dateFormatSeconds),
                    limit: moment.utc(endLimit).format(dateFormatSeconds),
                    holes: [],
                    count: 100
                }]
            };

            const config2: StartPointConfig = Object.assign({}, cloneDeep(config1), { id: 1 });

            const { dates: dates1, range: range1 } = determineStartingPoint(config1);

            expect(dates1.start.isSame(moment.utc(recoveryEndSlicerOne))).toBeTrue();
            expect(dates1.end.isSame(moment.utc(firstSegmentLimit))).toBeTrue();
            expect(dates1.limit.isSame(moment.utc(firstSegmentLimit))).toBeTrue();

            expect(range1.start.isSame(config1.dates.start)).toBeTrue();
            expect(range1.limit.isSame(config1.dates.limit)).toBeTrue();

            const { dates: dates2, range: range2 } = determineStartingPoint(config2);

            expect(dates2.start.isSame(moment.utc(recoveryEndSlicerTwo))).toBeTrue();
            expect(dates2.end.isSame(moment.utc(endLimit))).toBeTrue();
            expect(dates2.limit.isSame(moment.utc(endLimit))).toBeTrue();

            expect(range2.start.isSame(config2.dates.start)).toBeTrue();
            expect(range2.limit.isSame(config2.dates.limit)).toBeTrue();
        });
    });

    describe('for different slicer counts', () => {
        it('can return point and range when no recovery is given from 1 => 2 slicers no holes', () => {
            const interval: ParsedInterval = [1, 'm'];
            const limit = makeDate(dateFormatSeconds);
            const start = moment.utc(limit).subtract(2, 'm');
            const halfWay = moment.utc(start).add(interval[0], interval[1]);

            const recoveryStart = moment.utc(halfWay).subtract(30, 's');
            const recoveryEnd = moment.utc(recoveryStart).add(interval[0], interval[1]);

            const expectedRange = divideRange(recoveryEnd, limit, 2);

            const config: StartPointConfig = {
                id: 0,
                numOfSlicers: 2,
                dates: { start, limit },
                interval,
                recoveryData: [{
                    start: recoveryStart.format(dateFormatSeconds),
                    end: recoveryEnd.format(dateFormatSeconds),
                    limit: moment.utc(limit).format(dateFormatSeconds),
                    holes: [],
                    count: 100
                }]
            };

            const config2 = Object.assign({}, cloneDeep(config), { id: 1 });

            const { dates, range } = determineStartingPoint(config);

            expect(dates.start.isSame(moment.utc(recoveryEnd))).toBeTrue();
            expect(dates.end.isSame(moment.utc(expectedRange[0].limit))).toBeTrue();
            expect(dates.limit.isSame(moment.utc(expectedRange[0].limit))).toBeTrue();

            expect(range.start.isSame(config.dates.start)).toBeTrue();
            expect(range.limit.isSame(config.dates.limit)).toBeTrue();

            const { dates: dates2, range: range2 } = determineStartingPoint(config2);

            expect(dates2.start.isSame(moment.utc(expectedRange[1].start))).toBeTrue();
            expect(dates2.end.isSame(moment.utc(expectedRange[1].limit))).toBeTrue();
            expect(dates2.limit.isSame(moment.utc(expectedRange[1].limit))).toBeTrue();

            expect(range2.start.isSame(config.dates.start)).toBeTrue();
            expect(range2.limit.isSame(config.dates.limit)).toBeTrue();
        });

        it('can return point and range when no recovery is given from 1 => 2 slicers with holes covering all ranges', () => {
            const interval: ParsedInterval = [2, 'm'];
            const limit = makeDate(dateFormatSeconds);
            const start = moment.utc(limit).subtract(6, 'm');
            const step = moment.utc(start).add(interval[0], interval[1]);

            const recoveryStart = moment.utc(step).subtract(40, 's');
            const recoveryEnd = moment.utc(step).subtract(20, 's');

            const holeStart = moment.utc(recoveryEnd).format(dateFormatSeconds);
            const holeEnd = moment.utc(limit).format(dateFormatSeconds);
            const holes = [{ start: holeStart, end: holeEnd }];

            const config: StartPointConfig = {
                id: 0,
                numOfSlicers: 2,
                dates: { start, limit },
                interval,
                recoveryData: [{
                    start: recoveryStart.format(dateFormatSeconds),
                    end: recoveryEnd.format(dateFormatSeconds),
                    limit: moment.utc(limit).format(dateFormatSeconds),
                    holes,
                    count: 100
                }]
            };

            const config2 = Object.assign({}, cloneDeep(config), { id: 1 });

            const { dates, range } = determineStartingPoint(config);

            expect(dates.start.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates.end.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates.limit.isSame(moment.utc(holeStart))).toBeTrue();

            expect(range.start.isSame(config.dates.start)).toBeTrue();
            expect(range.limit.isSame(config.dates.limit)).toBeTrue();

            const { dates: dates2, range: range2 } = determineStartingPoint(config2);
            // if we cannot process anything we go straight to limit
            expect(dates2.start.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates2.end.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates2.limit.isSame(moment.utc(holeStart))).toBeTrue();
            expect(dates2.holes).toEqual([]);

            expect(range2.start.isSame(config.dates.start)).toBeTrue();
            expect(range2.limit.isSame(config.dates.limit)).toBeTrue();
        });
    });
});
