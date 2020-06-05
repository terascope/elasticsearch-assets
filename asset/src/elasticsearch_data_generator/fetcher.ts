import {
    Fetcher, WorkerContext, ExecutionConfig, TSError, AnyObject
} from '@terascope/job-components';
import mocker from 'mocker-data-generator';
import path from 'path';
import { existsSync } from 'fs';
import { DataGenerator, CounterResults } from './interfaces';
import defaultSchema from './data-schema';

export default class DataGeneratorFetcher extends Fetcher<DataGenerator> {
    dataSchema: any;

    constructor(context: WorkerContext, opConfig: DataGenerator, exConfig: ExecutionConfig) {
        super(context, opConfig, exConfig);
        this.dataSchema = parsedSchema(opConfig);
    }

    async fetch(slice?: CounterResults): Promise<AnyObject[]> {
        if (slice == null) return [];
        const { count } = slice;

        if (this.opConfig.stress_test) {
            return mocker()
                .schema('schema', this.dataSchema, 1)
                .build()
                .then((dataObj) => {
                    const results = [];
                    const data = dataObj.schema[0];
                    for (let i = 0; i < count; i += 1) {
                        results.push(data);
                    }
                    return results;
                })
                .catch((err) => Promise.reject(new TSError(err, { reason: 'could not generate mocked data' })));
        }

        return mocker()
            .schema('schema', this.dataSchema, count)
            .build()
            .then((dataObj) => dataObj.schema)
            .catch((err) => Promise.reject(new TSError(err, { reason: 'could not generate mocked data' })));
    }
}

function parsedSchema(opConfig: DataGenerator) {
    let dataSchema = {};

    if (opConfig.json_schema) {
        const firstPath = opConfig.json_schema;
        const nextPath = path.join(process.cwd(), opConfig.json_schema);

        try {
            if (existsSync(firstPath)) {
                dataSchema = require(firstPath);
            } else {
                dataSchema = require(nextPath);
            }
            return dataSchema;
        } catch (err) {
            throw new TSError(err, { reason: `Could not retrieve code for: ${opConfig._op}` });
        }
    } else {
        return defaultSchema(opConfig, dataSchema);
    }
}
