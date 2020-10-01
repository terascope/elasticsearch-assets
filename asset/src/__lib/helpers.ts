import {
    AnyObject,
    has,
    isSame
} from '@terascope/job-components';

export function getNonDefaultValues(config: AnyObject, schema: AnyObject): AnyObject {
    const results: AnyObject = {};

    for (const [key, value] of Object.entries(config)) {
        if (has(schema, key) && !isSame(schema[key], value)) {
            results[key] = value;
        }
    }

    return results;
}
