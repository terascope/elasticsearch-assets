import {
    ConvictSchema, ValidatedJobConfig, getOpConfig, get,
} from '@terascope/job-components';
import { BulkSender } from './interfaces';

export default class Schema extends ConvictSchema<BulkSender> {
    validateJob(job: ValidatedJobConfig) {
        const opConfig = getOpConfig(job, 'elasticsearch_bulk');
        if (opConfig == null) throw new Error('Could not find elasticsearch_bulk operation in jobConfig');
        const elasticConnectors = get(this.context, 'sysconfig.terafoundation.connectors.elasticsearch');
        if (elasticConnectors == null) throw new Error('Could not find elasticsearch connector in terafoundation config');

        // check to verify if connection map provided is
        // consistent with sysconfig.terafoundation.connectors
        if (opConfig.multisend) {
            for (const [, value] of Object.entries(opConfig.connection_map)) {
                if (!elasticConnectors[value]) {
                    throw new Error(`A connection for [${value}] was set on the elasticsearch_bulk connection_map but is not found in the system configuration [terafoundation.connectors.elasticsearch]`);
                }
            }
        }
    }

    build() {
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
            connection_map: {
                doc: 'Mapping from ID prefix to connection names. Routes data to multiple clusters '
                + 'based on the incoming key. Used when multisend is set to true. The key name can be a '
                + 'comma separated list of prefixes that will map to the same connection. Prefixes matching takes '
                + 'the first character of the key.',
                default: {
                    '*': 'default'
                },
                format: Object
            },
            multisend: {
                doc: 'When set to true the connection_map will be used allocate the data stream across multiple '
                + 'connections based on the keys of the incoming documents.',
                default: false,
                format: Boolean
            },
            multisend_index_append: {
                doc: 'When set to true will append the connection_map prefixes to the name of the index '
                + 'before data is submitted.',
                default: false,
                format: Boolean
            },
            connection: {
                doc: 'Name of the elasticsearch connection to use when sending data.',
                default: 'default',
                format: 'optional_String'
            }
        };
    }
}
