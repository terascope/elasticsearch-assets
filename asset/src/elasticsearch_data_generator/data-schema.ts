
import moment from 'moment';
import { DataGenerator } from './interfaces';
import { dateFormat } from '../helpers';

function regexID(type: any) {
    const reg = { randexp: '' };

    if (type === 'base64url') {
        // eslint-disable-next-line
        reg.randexp = '[a-zA-Z1-9\-\_]\w{8}';
    }
    if (type === 'hexadecimal') {
        reg.randexp = '[0-9a-f]{8}';
    }
    if (type === 'HEXADECIMAL') {
        reg.randexp = '[0-9A-F]{8}';
    }
    return reg;
}

function utcDate() {
    return new Date().toISOString();
}

function dateNow() {
    return moment().format(dateFormat);
}

function isoBetween(start: number, diff: number) {
    // ex.   "2016-01-19T13:48:08.426-07:00"
    return () => moment(start + (Math.random() * diff)).format(dateFormat);
}

function utcBetween(start: number, diff: number) {
    // ex.   "2016-01-19T20:48:08.426Z"  , compare to isoBetween, same dates
    return () => moment(start + (Math.random() * diff)).toISOString();
}

const formatOptions = {
    dateNow,
    isoBetween,
    utcDate,
    utcBetween
};

const nativeSchema = {
    ip: {
        faker: 'internet.ip'
    },
    userAgent: {
        faker: 'internet.userAgent'
    },
    url: {
        faker: 'internet.url'
    },
    uuid: {
        faker: 'random.uuid'
    },
    created: {
        function: dateNow
    },
    ipv6: {
        chance: 'ipv6'
    },
    location: {
        chance: 'coordinates'
    },
    bytes: {
        chance: 'integer({"min": 7850, "max": 5642867})'
    }
};

interface FormatOptions {
    start?: number;
    end?: number;
    diff?: number;
}
// TODO: use enum
function getFormatFunction(format: string, options: FormatOptions = {}) {
    const { start, diff } = options;
    if (format === 'isoBetween' || format === 'utcBetween') {
        return formatOptions[format](start as number, diff as number);
    }
    return formatOptions[format];
}

export default function getSchema(opConfig: DataGenerator, otherSchema: any) {
    const startDate = opConfig.start ? moment(opConfig.start) : moment(0); // 01 January, 1970 UTC
    const endDate = opConfig.end ? moment(opConfig.end) : moment();
    const schema = otherSchema || nativeSchema;
    const start = startDate.valueOf();
    const end = endDate.valueOf();
    const diff = end - start;

    if (opConfig.format) {
        schema[opConfig.date_key].function = getFormatFunction(opConfig.format, { start, diff });
    }

    if (opConfig.set_id) {
        schema.id = regexID(opConfig.set_id);
    }

    if (opConfig.id_start_key) {
        const reg = schema.id.randexp;
        schema.id.randexp = `${opConfig.id_start_key}${reg}`;
    }

    return schema;
}
