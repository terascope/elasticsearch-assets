import {
    ConvictSchema,
    AnyObject,
    cloneDeep,
    ValidatedJobConfig,
    APIConfig,
    getOpConfig
} from '@terascope/job-components';
import { ElasticsearchSenderConfig, DEFAULT_API_NAME } from './interfaces';
import { isValidIndex } from '../__lib/schema';
import { schema } from '../elasticsearch_bulk/schema';

const newSchema: AnyObject = cloneDeep(schema);

newSchema.size = {
    doc: 'the maximum number of docs it will take at a time, anything past it will be split up and sent'
    + 'note that the value should be even, the first doc will be the index data and then the next is the data',
    default: 500,
    format(val: any) {
        if (isNaN(val)) {
            throw new Error('Invalid size parameter for elasticsearch_sender_api opConfig, it must be a number');
        } else if (val <= 0) {
            throw new Error('Invalid size parameter for elasticsearch_sender_api, it must be greater than zero');
        }
    }
};
// TODO: tests for this
newSchema.index.format = (val: unknown) => {
    isValidIndex(val);
};

export default class Schema extends ConvictSchema<ElasticsearchSenderConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const apiConfigs = job.apis.filter((config) => {
            const apiName = config._name;
            return apiName === DEFAULT_API_NAME || apiName.startsWith(`${DEFAULT_API_NAME}:`);
        });

        const { connectors } = this.context.sysconfig.terafoundation;

        // hack to get around default connection check until schema updates for routed_sender
        // check the first connection on the routed sender op
        if (connectors.elasticsearch.default == null && getOpConfig(job, 'routed_sender')) {
            this._applyRoutedSenderConnection(job, apiConfigs);
        }

        apiConfigs.forEach((apiConfig: AnyObject) => {
            const { connection } = apiConfig;

            const endpointConfig = connectors.elasticsearch[connection];

            if (endpointConfig == null) throw new Error(`Could not find elasticsearch endpoint configuration for connection ${connection}`);
        });
    }

    _applyRoutedSenderConnection(job: ValidatedJobConfig, apiConfigs: APIConfig[]): void {
        job.operations.forEach((op) => {
            if (op._op === 'routed_sender') {
                apiConfigs.filter((config) => config._name === op.api_name)
                    .forEach((config) => { [config.connection] = Object.values(op.routing); });
            }
        });
    }

    build(): AnyObject {
        return newSchema;
    }
}
