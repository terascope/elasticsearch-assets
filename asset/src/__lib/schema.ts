import { getTypeOf, isString, isNotNil } from '@terascope/job-components';
import { DEFAULT_API_NAME as ES_DEFAULT_API_NAME } from '../elasticsearch_reader_api/interfaces';
import { DEFAULT_API_NAME as SPACES_DEFAULT_API_NAME } from '../spaces_reader_api/interfaces';

export const elasticOpSchema = {
    api_name: {
        doc: 'name of api to be used by spaces reader',
        default: null,
        format: (val: unknown): void => {
            if (isNotNil(val)) {
                if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
                if (!val.includes(ES_DEFAULT_API_NAME)) throw new Error('Invalid parameter api_name, it must be an spaces_reader_api');
            }
        }
    }
};

export const spacesOpSchema = {
    api_name: {
        doc: 'name of api to be used by spaces reader',
        default: null,
        format: (val: unknown): void => {
            if (isNotNil(val)) {
                if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
                if (!val.includes(SPACES_DEFAULT_API_NAME)) throw new Error('Invalid parameter api_name, it must be an spaces_reader_api');
            }
        }
    }
};
