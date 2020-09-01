import { ConvictSchema, AnyObject, cloneDeep } from '@terascope/job-components';
import { spacesSchema } from '../spaces_reader/schema';
import { ApiConfig } from '../elasticsearch_reader/interfaces';

const clone = cloneDeep(spacesSchema) as AnyObject;
// this should not continue onward
delete clone.api_name;

clone.index.format = 'required_String';

export default class Schema extends ConvictSchema<ApiConfig> {
    build(): AnyObject {
        return clone;
    }
}
