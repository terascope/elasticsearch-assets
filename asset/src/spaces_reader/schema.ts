import { BaseSchema } from '@terascope/job-components';
import { AssetSpacesAPIConfig } from './interfaces.js';
import { opSchema } from '../__lib/schema.js';

export default class Schema extends BaseSchema<AssetSpacesAPIConfig> {
    build() {
        return opSchema;
    }
}
