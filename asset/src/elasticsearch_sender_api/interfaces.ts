import { APIConfig } from '@terascope/job-components';

export interface SenderConfig extends APIConfig {
    connection?: string
    index?: string;
    size?: number;
    full_response?: boolean
}
