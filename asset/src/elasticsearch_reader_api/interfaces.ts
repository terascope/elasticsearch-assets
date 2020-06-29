import { APIConfig, APIFactoryRegistry, AnyObject } from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';

export const DEFAULT_API_NAME = 'elasticsearch_reader_api';

export type ElasticReaderFactoryAPI = APIFactoryRegistry<elasticAPI.Client, AnyObject>

export interface ReaderConfig extends APIConfig {
    connection?: string
    index?: string;
    full_response?: boolean
}

export interface ValidReaderConfig extends APIConfig {
    connection: string
    index?: string;
    full_response?: boolean
}
