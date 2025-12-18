import { has, tryParseJSON, TSError } from '@terascope/core-utils';
import { inspect } from 'node:util';

/**
 * Try and proxy the error as best as possible to avoid confusing errors
*/
export function throwRequestError(endpointName: string, statusCode: number, body: unknown): never {
    const resp = tryParseJSON(body);

    if (resp && has(resp, 'error')) {
        if (resp.debug) {
            const err = new Error(resp.debug.message);
            if (resp.debug.stack) {
                err.stack = resp.debug.stack;
            }
            if (resp.debug.statusCode) {
                // @ts-expect-error
                err.statusCode = resp.debug.statusCode;
            }
            throw new TSError(err, {
                defaultStatusCode: statusCode,
                reason: resp.error,
                context: {
                    endpoint: endpointName,
                    safe: false
                }
            });
        } else {
            throw new TSError(resp.error, {
                statusCode,
                context: {
                    endpoint: endpointName,
                    safe: true
                }
            });
        }
    }

    // fallback error handling
    throw new TSError(`Unknown Request Error: ${inspect(resp, { depth: 10 })}`, {
        statusCode
    });
}
