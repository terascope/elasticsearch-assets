import {
    has, tryParseJSON, TSError
} from '@terascope/utils';
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
            let errorMessage = resp.error;

            // Check if this is an elasticsearch error that should be surfaced to the user
            if (typeof resp.error === 'string') {
                if (resp.error.includes('search_phase_execution_exception')
                    || resp.error.includes('too_many_clauses')
                    || resp.error.includes('maxClauseCount')) {
                    errorMessage = `Elasticsearch query failed: ${resp.error}`;
                }
            }

            // Check if the error is in a nested structure common in elasticsearch responses
            if (resp.error && typeof resp.error === 'object') {
                const nestedError = resp.error;
                if (nestedError.type === 'search_phase_execution_exception'
                    || (nestedError.reason && nestedError.reason.includes('too_many_clauses'))) {
                    errorMessage = `Elasticsearch query failed: ${nestedError.reason || nestedError.type}`;
                }
            }

            throw new TSError(errorMessage, {
                statusCode,
                context: {
                    endpoint: endpointName,
                    safe: typeof errorMessage === 'string' && errorMessage.startsWith('Elasticsearch query failed') ? false : true
                }
            });
        }
    }

    // fallback error handling
    throw new TSError(`Unknown Request Error: ${inspect(resp, { depth: 10 })}`, {
        statusCode
    });
}
