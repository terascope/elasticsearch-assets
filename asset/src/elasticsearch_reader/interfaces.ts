import { OpConfig } from '@terascope/job-components';
import { ESReaderOptions, SpacesAPIConfig } from '@terascope/elasticsearch-asset-apis';

export interface ESReaderConfig extends ESReaderOptions, OpConfig {
    api_name: string;
}

export type ESDateConfig = ESReaderConfig | SpacesAPIConfig;
