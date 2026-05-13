import { ESReaderOptions, SpacesAPIConfig } from '@terascope/elasticsearch-asset-apis';
import { OpConfig } from '@terascope/job-components';

export interface SharedReaderConfig extends ESReaderOptions, SpacesAPIConfig {}

export interface OpApiConfig extends OpConfig {
    _api_name: string;
}
