import { APIConfig } from '@terascope/job-components';

export interface SenderConfig extends APIConfig {
    connection?: string
    index?: string;
    size?: number;
    full_response?: boolean
}

export interface ValidSenderConfig extends APIConfig {
    connection: string
    size: number;
    index?: string;
    full_response?: boolean
}
