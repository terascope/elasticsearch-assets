import { ConvictSchema, AnyObject, ValidatedJobConfig } from '@terascope/job-components';
import { AssetSpacesAPIConfig } from './interfaces';
import { opSchema } from '../__lib/schema';
import { DEFAULT_API_NAME } from '../spaces_reader_api/interfaces';

export default class Schema extends ConvictSchema<AssetSpacesAPIConfig> {
    validateJob(job: ValidatedJobConfig): void {
        let opIndex = 0;

        const opConfig = job.operations.find((op, ind) => {
            if (op._op === 'spaces_reader') {
                opIndex = ind;
                return op;
            }
            return false;
        });

        if (opConfig == null) throw new Error('Could not find spaces_reader operation in jobConfig');

        const { api_name, ...apiConfig } = opConfig;

        const apiName = api_name || `${DEFAULT_API_NAME}:${opConfig._op}-${opIndex}`;

        // we set the new apiName back on the opConfig so it can reference the unique name
        opConfig.api_name = apiName;

        this.ensureAPIFromConfig(apiName, job, apiConfig);
    }

    build(): AnyObject {
        return opSchema;
    }
}
