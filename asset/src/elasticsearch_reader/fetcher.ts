import { WorkerContext, getClient, ExecutionConfig } from '@terascope/job-components';
import { ESDateConfig } from './interfaces';
import ESDateFetcher from './elasticsearch_date_range/reader';

export default class ESReader extends ESDateFetcher {
    constructor(
        context: WorkerContext,
        opConfig: ESDateConfig,
        executionConfig: ExecutionConfig
    ) {
        const client = getClient(context, opConfig, 'elasticsearch');
        super(context, opConfig, executionConfig, client);
    }
}
