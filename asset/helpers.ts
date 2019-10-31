// @ts-ignore
import parseError from '@terascope/error-parser';
import { Logger } from '@terascope/job-components';
import fs from 'fs';

export function dateOptions(value: string) {
    const options = {
        year: 'y',
        years: 'y',
        y: 'y',
        months: 'M',
        month: 'M',
        mo: 'M',
        mos: 'M',
        M: 'M',
        weeks: 'w',
        week: 'w',
        wks: 'w',
        wk: 'w',
        w: 'w',
        days: 'd',
        day: 'd',
        d: 'd',
        hours: 'h',
        hour: 'h',
        hr: 'h',
        hrs: 'h',
        h: 'h',
        minutes: 'm',
        minute: 'm',
        min: 'm',
        mins: 'm',
        m: 'm',
        seconds: 's',
        second: 's',
        s: 's',
        milliseconds: 'ms',
        millisecond: 'ms',
        ms: 'ms'
    };

    if (options[value]) {
        return options[value];
    }

    throw new Error(`the time descriptor of "${value}" for the interval is malformed`);
}


// "2016-01-19T13:33:09.356-07:00"
export const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSSZ';

// 2016-06-29T12:44:57-07:00
export const dateFormatSeconds = 'YYYY-MM-DDTHH:mm:ssZ';

type RetryFn = (msg: string) => any

export function retryModule(logger: Logger, numOfRetries: number) {
    const retry = {};
    return (key: string, err: Error, fn: RetryFn, msg: string) => {
        const errMessage = parseError(err);
        logger.error('error while getting next slice', errMessage);

        if (!retry[key]) {
            retry[key] = 1;
            return fn(msg);
        }

        retry[key] += 1;
        if (retry[key] > numOfRetries) {
            return Promise.reject(
                new Error(`max_retries met for slice, key: ${key}`)
            );
        }

        return fn(msg);
    };
}

export function existsSync(filename: string) {
    try {
        fs.accessSync(filename);
        return true;
    } catch (ex) {
        return false;
    }
}
