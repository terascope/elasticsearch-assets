import {
    ConvictSchema,
    AnyObject,
    isString,
    getTypeOf,
    getOpConfig,
    ValidatedJobConfig,
    isNil
} from '@terascope/job-components';
import ReaderSchema from '../elasticsearch_reader/schema';
import { ApiConfig } from '../elasticsearch_reader/interfaces';
import { DEFAULT_API_NAME } from '../spaces_reader_api/interfaces';

export default class Schema extends ConvictSchema<ApiConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = getOpConfig(job, 'simple_api_reader') as ApiConfig;
        const { api_name, ...apiConfig } = opConfig;
        if (!Array.isArray(job.apis)) job.apis = [];
        const SpacesReaderAPI = job.apis.find((jobApi) => jobApi._name === api_name);

        if (isNil(SpacesReaderAPI)) {
            if (isNil(opConfig.index)) throw new Error('Invalid elasticsearch_reader configuration, must provide parameter index');

            job.apis.push({
                _name: DEFAULT_API_NAME,
                ...apiConfig
            });
        }
    }

    build(): AnyObject {
        const schema = new ReaderSchema(this.context, this.opType);
        const esSchema = schema.build();
        esSchema.api_name = {
            doc: 'name of api to be used by elasticearch reader',
            default: DEFAULT_API_NAME,
            format: (val: unknown) => {
                if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
                if (!val.includes(DEFAULT_API_NAME)) throw new Error('Invalid parameter api_name, it must be an spaces_reader_api');
            }
        };

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
        console.log('what is esSchema', esSchema)
        return Object.assign({}, esSchema, apiSchema);
    }
}
