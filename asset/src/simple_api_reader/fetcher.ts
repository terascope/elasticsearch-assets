import { WorkerContext, makeExContextLogger, ExecutionConfig } from '@terascope/job-components';
import ESDateFetcher from '../elasticsearch_reader/elasticsearch_date_range/reader';
import { ApiConfig } from '../elasticsearch_reader/interfaces';

export default class SpacesReader extends ESDateFetcher {
    constructor(
        context: WorkerContext,
        opConfig: ApiConfig,
        executionConfig: ExecutionConfig
    ) {
        if (opConfig.api_name)
        super(context, opConfig, executionConfig);
    }
}
