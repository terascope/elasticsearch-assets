import {
    getClient,
    WorkerContext,
    ExecutionConfig,
} from '@terascope/job-components';

import { ESReaderConfig } from './interfaces';

import DateSlicer from './elasticsearch_date_range/slicer';

export default class ESDateSlicer extends DateSlicer {
    constructor(
        context: WorkerContext,
        opConfig: ESReaderConfig,
        executionConfig: ExecutionConfig
    ) {
        const client = getClient(context, opConfig, 'elasticsearch');
        super(context, opConfig, executionConfig, client);
    }
}
