import { isString } from '@terascope/core-utils';

export const opSchema = {
    _api_name: {
        doc: 'name of api to be used by spaces reader',
        default: null,
        format: 'optional_string'
    }
};

export function isValidIndex(index: unknown): void {
    if (!isString(index)) throw new Error('Invalid index parameter, must be of type string');
    if (index.length === 0) throw new Error('Invalid index parameter, must not be an empty string');
    if (index.match(/[A-Z]/)) throw new Error('Invalid index parameter, must be lowercase');
}
