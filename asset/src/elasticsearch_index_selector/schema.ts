import {
    ConvictSchema, ValidatedJobConfig, getOpConfig
} from '@terascope/job-components';
import { IndexSelectorConfig } from './interfaces';

export default class Schema extends ConvictSchema<IndexSelectorConfig> {
    validateJob(job: ValidatedJobConfig) {
        const opConfig = getOpConfig(job, 'elasticsearch_index_selector');
        const preserveId = job.operations.find((op) => op.preserve_id === true);

        if (opConfig == null) throw new Error('could not find elasticsearch_index_selector operation in jobConfig');

        if (opConfig.timeseries || opConfig.index_prefix || opConfig.date_field) {
            if (!(opConfig.timeseries && opConfig.index_prefix && opConfig.date_field)) {
                throw new Error('elasticsearch_index_selector is mis-configured, if any of the following configurations are set: timeseries, index_prefix or date_field, they must all be used together, please set the missing parameters');
            }
        }

        if (!opConfig.type && !preserveId) {
            throw new Error('type must be specified in elasticsearch index selector config if data is not a full response from elasticsearch');
        }
    }

    build() {
        return {
            index: {
                doc: 'Index to where the data will be sent to, if you wish the index to be based on a timeseries, '
                  + 'use the timeseries option instead',
                default: '',
                format(val: any) {
                    if (typeof val !== 'string') {
                        throw new Error('index must be of type string');
                    }

                    if (val.match(/[A-Z]/)) {
                        throw new Error('index must be lowercase');
                    }

                    if (val.length === 0) {
                        throw new Error('index must not be an empty string');
                    }
                }
            },
            type: {
                doc: 'Set the type of the data for elasticsearch. If incoming data is from elasticsearch'
                + ' it will default to the type on the metadata if this field is not set. This field must be set'
                + 'for all other incoming data',
                default: '',
                format: 'optional_String'
            },
            preserve_id: {
                doc: 'If incoming data if from elasticsearch, set this to true if you wish to keep the previous id'
                + ' else elasticsearch will generate one for you (upload performance is faster if you let it auto-generate)',
                default: false,
                format: Boolean
            },
            id_field: {
                doc: 'If you wish to set the id based off another field in the doc, set the name of the field here',
                default: '',
                format: 'optional_String'
            },
            timeseries: {
                doc: 'Set to either daily, weekly, monthly or yearly if you want the index to be based off it, must be '
                + 'used in tandem with index_prefix and date_field',
                default: '',
                format(value: any) {
                    // This will generate logstash style timeseries names
                    if (value && (!['daily', 'weekly', 'monthly', 'yearly'].includes(value))) {
                        throw new Error("timeseries must be one of 'daily', 'weekly', 'monthly', 'yearly'");
                    }
                }
            },
            index_prefix: {
                doc: 'Used with timeseries, adds a prefix to the date ie (index_prefix: "events-" ,timeseries: "daily => '
                + 'events-2015.08.20',
                default: '',
                format(val: any) {
                    if (val) {
                        if (typeof val !== 'string') {
                            throw new Error('index_prefix must be of type string');
                        }
                        if (val.match(/[A-Z]/)) {
                            throw new Error('index_prefix must be lowercase');
                        }
                    }
                }
            },
            date_field: {
                doc: 'Used with timeseries, specify what field of the data should be used to calculate the timeseries',
                default: '',
                format: 'optional_String'
            },
            delete: {
                doc: 'Use the id_field from the incoming records to bulk delete documents.',
                default: false,
                format: Boolean
            },
            update: {
                doc: 'Specify if the data should update the records or, if not it will index them',
                default: false,
                format: Boolean
            },
            update_retry_on_conflict: {
                doc: 'If there is a version conflict from an update how often should it be retried.',
                default: 0,
                format(val: any) {
                    if (isNaN(val)) {
                        throw new Error('update_retry_on_conflict for elasticsearch_bulk must be a number');
                    } else if (val < 0) {
                        throw new Error('update_retry_on_conflict for elasticsearch_bulk must be greater than or equal to zero');
                    }
                }
            },
            update_fields: {
                doc: 'if you are updating the documents, you can specify fields to update here (it should be an array '
                + 'containing all the field names you want updated), it defaults to sending the entire document',
                default: [],
                format: Array
            },
            upsert: {
                doc: 'Specify if the incoming records should be used to perform an upsert. If update_fields is also specified then existing records will be updated with those fields otherwise the full incoming record will be inserted.',
                default: false,
                format: Boolean
            },
            create: {
                doc: 'Specify if the incoming records should be used to perform an create event ("put-if-absent" behavior).',
                default: false,
                format: Boolean
            },
            script_file: {
                doc: 'Name of the script file to run as part of an update request.',
                default: '',
                format: 'optional_String'
            },
            script: {
                doc: 'Inline script to include in each indexing request. Only very simple painless scripts are currently supported.',
                default: '',
                format: 'optional_String'
            },
            script_params: {
                doc: 'key -> value parameter mappings. The value will be extracted from the incoming data and passed to the script as param based on the key.',
                default: {},
                format: Object
            }
        };
    }
}
