import { times } from '@terascope/utils';
import { DataTypeFields } from '@terascope/types';

const baseDate = new Date('2019-04-26T08:00:23.207-07:00').valueOf();
let counter = 0;
let millisecondTimer = 0;
// this created 2000 records in the date span of 2 milliseconds
const data = times(2_000, (index) => {
    // 5000 record chunk per milliseconds
    if ((counter % 1_000) === 0) millisecondTimer += 1;
    const newDate = new Date(baseDate + millisecondTimer).toISOString();
    counter += 1;

    const results = {
        bytes: index,
        created: newDate
    };
    return results;
});

const types: DataTypeFields = {
    created: { type: 'Date' },
    bytes: { type: 'Integer' }
};

export = {
    data,
    types,
    index: 'condensed_spread'
}
