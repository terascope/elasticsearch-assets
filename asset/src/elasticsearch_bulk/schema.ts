import {
    ConvictSchema,
    ValidatedJobConfig,
    getOpConfig,
    get,
    AnyObject,
    isNil,
    isString,
    getTypeOf
} from '@terascope/job-components';
import { BulkSender } from './interfaces';
import { DEFAULT_API_NAME } from '../elasticsearch_sender_api/interfaces';

function fetchConfig(job: ValidatedJobConfig) {
    const opConfig = getOpConfig(job, 'elasticsearch_bulk');
    if (opConfig == null) throw new Error('Could not find elasticsearch_bulk operation in jobConfig');
    return opConfig as BulkSender;
}

export default class Schema extends ConvictSchema<BulkSender> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = fetchConfig(job);
        const elasticConnectors = get(this.context, 'sysconfig.terafoundation.connectors.elasticsearch');
        if (elasticConnectors == null) throw new Error('Could not find elasticsearch connector in terafoundation config');

        const {
            index, connection, size, api_name
        } = opConfig;
        if (!Array.isArray(job.apis)) job.apis = [];
        const ElasticSenderAPI = job.apis.find((jobApi) => jobApi._name === api_name);

        if (isNil(ElasticSenderAPI)) {
            if (isNil(opConfig.index)) throw new Error('Invalid elasticsearch_reader configuration, must provide parameter index');

            job.apis.push({
                _name: DEFAULT_API_NAME,
                index,
                connection,
                size
            });
        }
    }

    build(): AnyObject {
        return {
            size: {
                doc: 'the maximum number of docs it will take at a time, anything past it will be split up and sent'
                + 'note that the value should be even, the first doc will be the index data and then the next is the data',
                default: 500,
                format(val: any) {
                    if (isNaN(val)) {
                        throw new Error('Invalid size parameter for elasticsearch_bulk opConfig, it must be a number');
                    } else if (val <= 0) {
                        throw new Error('Invalid size parameter for elasticsearch_bulk, it must be greater than zero');
                    }
                }
            },
            connection: {
                doc: 'Name of the elasticsearch connection to use when sending data.',
                default: 'default',
                format: 'optional_String'
            },
            api_name: {
                doc: 'name of api to be used by elasticearch reader',
                default: DEFAULT_API_NAME,
                format: (val: unknown) => {
                    if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
                    if (!val.includes(DEFAULT_API_NAME)) throw new Error('Invalid parameter api_name, it must be an elasticsearch_reader_api');
                }
            }
        };
    }
}
