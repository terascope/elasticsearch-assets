import { OpConfig } from '@terascope/job-components';
import { ESReaderOptions } from '@terascope/elasticsearch-asset-apis';

export interface ESReaderConfig extends Partial<ESReaderOptions>, OpConfig {}
