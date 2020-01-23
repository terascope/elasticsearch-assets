import { makeExContextLogger, WorkerContext, ExecutionConfig } from '@terascope/job-components';
import MockedClient from './client';
import { ApiConfig } from '../elasticsearch_reader/interfaces';
import DateSlicer from '../elasticsearch_reader/elasticsearch_date_range/slicer';

export default class ESDateSlicer extends DateSlicer {
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
