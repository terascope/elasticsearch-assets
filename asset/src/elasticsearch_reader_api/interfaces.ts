import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { ESReaderOptions, ElasticsearchReaderAPI } from '@terascope/elasticsearch-asset-apis';

export const DEFAULT_API_NAME = 'elasticsearch_reader_api';
export interface ElasticsearchReaderAPIConfig extends ESReaderOptions, APIConfig {}
export type ElasticReaderFactoryAPI = APIFactoryRegistry<
    ElasticsearchReaderAPI, ElasticsearchReaderAPIConfig
>;
