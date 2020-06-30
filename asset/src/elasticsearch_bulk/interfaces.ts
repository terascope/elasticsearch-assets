import { OpConfig } from '@terascope/job-components';

export interface BulkSender extends OpConfig {
    size: number;
    connection: string;
    api_name: string;
}
