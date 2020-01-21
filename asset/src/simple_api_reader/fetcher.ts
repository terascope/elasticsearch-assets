import { WorkerContext, makeExContextLogger, ExecutionConfig } from '@terascope/job-components';

import ESDateFetcher from '../elasticsearch_reader/elasticsearch_date_range/reader';
import MockedClient from './client';
import { ApiConfig } from '../elasticsearch_reader/interfaces';

export default class ESReader extends ESDateFetcher {
    constructor(
        context: WorkerContext,
        opConfig: ApiConfig,
        executionConfig: ExecutionConfig
    ) {
        const logger = makeExContextLogger(context, executionConfig, 'operation', {
            opName: opConfig._op,
        });
        const client = new MockedClient(opConfig, logger);
        super(context, opConfig, executionConfig, client);
    }
}
