import { makeExContextLogger, WorkerContext, ExecutionConfig } from '@terascope/job-components';
import { Client } from 'elasticsearch';
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
        const client = new MockedClient(opConfig, logger) as unknown as Client;
        super(context, opConfig, executionConfig, client);
    }
}
