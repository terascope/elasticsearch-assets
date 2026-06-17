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
            let errorMessage = resp.error;
            let isElasticsearchError = false;

            // Handle elasticsearch errors - these should always be surfaced to users
            if (typeof resp.error === 'string') {
                // Any string error from elasticsearch should be clearly formatted
                errorMessage = `Elasticsearch query failed: ${resp.error}`;
                isElasticsearchError = true;
            }

            // Handle nested elasticsearch error structure common in elasticsearch responses
            if (resp.error && typeof resp.error === 'object') {
                const nestedError = resp.error;
                if (nestedError.type || nestedError.reason) {
                    // Format structured elasticsearch errors
                    const type = nestedError.type || 'elasticsearch_error';
                    const reason = nestedError.reason || 'Unknown error';
                    errorMessage = `Elasticsearch query failed: ${type}: ${reason}`;
                    isElasticsearchError = true;
                } else {
                    // Generic object error
                    errorMessage = `Elasticsearch query failed: ${JSON.stringify(resp.error)}`;
                    isElasticsearchError = true;
                }
            }

            throw new TSError(errorMessage, {
                statusCode,
                context: {
                    endpoint: endpointName,
                    safe: !isElasticsearchError
                }
            });
        }
    }

    // fallback error handling
    throw new TSError(`Unknown Request Error: ${inspect(resp, { depth: 10 })}`, {
        statusCode
    });
}
