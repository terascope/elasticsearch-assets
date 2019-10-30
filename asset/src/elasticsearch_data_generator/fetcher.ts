
import {
    Fetcher, SliceRequest, WorkerContext, ExecutionConfig, toString
} from '@terascope/job-components';
import mocker from 'mocker-data-generator';
import path from 'path';
import { existsSync } from 'fs';
import { DataGenerator } from './interfaces';

export default class DataGeneratorFetcher extends Fetcher<DataGenerator> {
    dataSchema: any;

    constructor(context: WorkerContext, opConfig: DataGenerator, exConfig: ExecutionConfig) {
        super(context, opConfig, exConfig);
        this.dataSchema = parsedSchema(opConfig);
    }
    // TODO: is this right type here?
    async fetch(slice?: SliceRequest) {
        const numberOfDoc = slice;
        if (slice == null) return [];

        if (this.opConfig.stress_test) {
            return mocker()
                .schema('schema', this.dataSchema, 1)
                .build()
                .then((dataObj) => {
                    const results = [];
                    const data = dataObj.schema[0];
                    // @ts-ignore TODO: review this
                    for (let i = 0; i < numberOfDoc; i += 1) {
                        results.push(data);
                    }
                    return results;
                })
                .catch((err) => Promise.reject(new Error(`could not generate data error: ${toString(err)}`)));
        }

        return mocker()
            .schema('schema', this.dataSchema, slice)
            .build()
            .then((dataObj) => dataObj.schema)
            .catch((err) => Promise.reject(new Error(`could not generate data error: ${toString(err)}`)));
    }
}

function parsedSchema(opConfig: DataGenerator) {
    let dataSchema = false;

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
        } catch (e) {
            throw new Error(`Could not retrieve code for: ${opConfig}\n${e}`);
        }
    } else {
        return require('./default_schema')(opConfig, dataSchema);
    }
}
