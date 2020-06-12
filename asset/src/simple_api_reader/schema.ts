import { ConvictSchema, AnyObject } from '@terascope/job-components';
import ReaderSchema from '../elasticsearch_reader/schema';
import { ApiConfig } from '../elasticsearch_reader/interfaces';

export default class Schema extends ConvictSchema<ApiConfig> {
    build(): AnyObject {
        const schema = new ReaderSchema(this.context, this.opType);
        const esSchema = schema.build();
        const apiSchema = {
            endpoint: {
                doc: 'The base API endpoint to read from: i.e. http://yourdomain.com/api/v1',
                default: '',
                format: 'required_String'
            },
            token: {
                doc: 'API access token for making requests',
                default: '',
                format: 'required_String'
            },
            timeout: {
                doc: 'Time in milliseconds to wait for a connection to timeout.',
                default: 300000
            },
        };

        return Object.assign({}, esSchema, apiSchema);
    }
}
