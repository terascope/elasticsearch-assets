import { ConvictSchema } from '@terascope/job-components';
import { AnyObject } from '@terascope/utils';

export default class Schema extends ConvictSchema<AnyObject> {
    build(): AnyObject {
        return {};
    }
}
