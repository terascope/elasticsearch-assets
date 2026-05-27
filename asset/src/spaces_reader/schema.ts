import { BaseSchema } from '@terascope/job-components';
import { OpApiConfig } from '../__lib/interfaces.js';
import { opSchema } from '../__lib/schema.js';

export default class Schema extends BaseSchema<OpApiConfig> {
    build() {
        return opSchema;
    }
}
