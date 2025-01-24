import { OpConfig } from '@terascope/job-components';
import { SpacesAPIConfig } from '@terascope/elasticsearch-asset-apis';

export interface AssetSpacesAPIConfig extends OpConfig, SpacesAPIConfig {
    connection: string;
}
