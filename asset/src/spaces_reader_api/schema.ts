import { ConvictSchema, AnyObject, cloneDeep } from '@terascope/job-components';
import { spacesSchema } from '../spaces_reader/schema';
import { ApiConfig } from '../elasticsearch_reader/interfaces';

const clone = cloneDeep(spacesSchema);
// this should not continue onward
delete clone.api_name;

export default class Schema extends ConvictSchema<ApiConfig> {
    build(): AnyObject {
        return clone;
    }
}
