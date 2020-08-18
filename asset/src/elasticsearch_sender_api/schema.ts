import {
    ConvictSchema, AnyObject, cloneDeep,
} from '@terascope/job-components';
import { ElasticsearchSenderConfig } from './interfaces';

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

export default class Schema extends ConvictSchema<ElasticsearchSenderConfig> {
    build(): AnyObject {
        return newSchema;
    }
}
