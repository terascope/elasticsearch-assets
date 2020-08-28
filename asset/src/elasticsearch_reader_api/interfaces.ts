import { APIConfig, APIFactoryRegistry, AnyObject } from '@terascope/job-components';
import Reader from './reader';
import { ESReaderOptions } from '../elasticsearch_reader/interfaces';

export const DEFAULT_API_NAME = 'elasticsearch_reader_api';

export type ElasticReaderFactoryAPI = APIFactoryRegistry<Reader, AnyObject>
export interface ElasticsearchReaderAPIConfig extends ESReaderOptions, APIConfig {}
