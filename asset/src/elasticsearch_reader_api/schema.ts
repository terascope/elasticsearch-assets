import {
    ConvictSchema, AnyObject,
} from '@terascope/job-components';
import { ReaderConfig } from './interfaces';

export default class Schema extends ConvictSchema<ReaderConfig> {
    build(): AnyObject {
        return {
            connection: {
                doc: 'Name of the elasticsearch connection to use when sending data.',
                default: 'default',
                format: 'optional_String'
            },
            full_response: {
                doc: 'Set to true if want the full elasticsearch response back',
                default: false,
                format: Boolean
            },
            index: {
                doc: 'Name of index to preform elasticsearch-api actions on, only used in a few cases',
                default: undefined,
                format: 'optional_String'
            },
        };
    }
}
