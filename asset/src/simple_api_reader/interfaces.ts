import { ESReaderConfig } from '../elasticsearch_reader/interfaces';

export interface ApiConfig extends ESReaderConfig {
    endpoint: string;
    token: string;
    timeout: number;
}
