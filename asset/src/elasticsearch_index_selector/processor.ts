import {
    DataEntity,
    BatchProcessor,
    AnyObject,
    set,
    getValidDate,
    TSError
} from '@terascope/job-components';
import { IndexSelectorConfig } from './interfaces';

function _getWeeklyIndex(date: string) {
    // weeks since Jan 1, 1970
    return Math.floor(Date.parse(date) / 604800000);
}
const offsets = {
    daily: 10,
    monthly: 7,
    yearly: 4
};

interface BulkMeta {
    _index: string;
    _type: string;
    _id: string | number;
    retry_on_conflict: number;
}

interface IndexSpec extends DataEntity {
    index?: AnyObject;
    create?: AnyObject;
    update?: AnyObject;
    delete?: AnyObject;
}

interface ScriptConfig {
    file?: string;
    source?: string;
    params?: AnyObject;
}

interface UpdateConfig extends DataEntity {
    upsert?: AnyObject;
    doc?: AnyObject;
    script?: ScriptConfig;
}

export default class IndexSelector extends BatchProcessor<IndexSelectorConfig> {
    private formattedDate(record: DataEntity) {
        const { date_field: dateField, timeseries } = this.opConfig;
        let end = 10;

        const recordData = getValidDate(record[dateField as string]);
        if (!recordData) throw new TSError(`Invalid opConfig date field: ${dateField}. It either does not exists or is not a valid date on the records processed`);
        let date = recordData.toISOString();

        if (timeseries && typeof timeseries === 'string') {
            if (timeseries === 'weekly') {
                return _getWeeklyIndex(date);
            }
            end = offsets[timeseries];
        }

        date = date.slice(0, end);
        return date.replace(/-/gi, '.');
    }

    private indexName(record: DataEntity) {
        if (this.opConfig.timeseries) {
            const index = this.formattedDate(record);
            const prefix = this.opConfig.index_prefix as string;
            const indexPrefix = prefix.charAt(prefix.length - 1) === '-' ? prefix : `${prefix}-`;

            return indexPrefix + index;
        }

        return this.opConfig.index;
    }

    private generateRequest(record: DataEntity, formatted: DataEntity[]) {
        const indexSpec: IndexSpec = DataEntity.make({});
        const index = this.indexName(record);
        const meta: Partial<BulkMeta> = {
            _index: index
        };

        if (this.opConfig.type) meta._type = this.opConfig.type;

        if (this.opConfig.preserve_id) meta._id = record.getKey();

        if (this.opConfig.id_field) meta._id = record[this.opConfig.id_field];

        if (this.opConfig.update || this.opConfig.upsert) {
            indexSpec.update = meta;

            if (this.opConfig.update_retry_on_conflict > 0) {
                meta.retry_on_conflict = this.opConfig.update_retry_on_conflict;
            }
        } else if (this.opConfig.delete) {
            indexSpec.delete = meta;
        } else if (this.opConfig.create) {
            indexSpec.create = meta;
        } else {
            indexSpec.index = meta;
        }

        formatted.push(indexSpec);

        if (this.opConfig.update || this.opConfig.upsert) {
            const update: UpdateConfig = DataEntity.make({});

            if (this.opConfig.upsert) {
                // The upsert field is what is inserted if the key doesn't already exist
                update.upsert = record;
            }

            // This will merge this record with the existing record.
            if (this.opConfig.update_fields.length > 0) {
                update.doc = {};
                this.opConfig.update_fields.forEach((field) => {
                    // @ts-ignore
                    update.doc[field] = record[field];
                });
            } else if (this.opConfig.script_file || this.opConfig.script) {
                if (this.opConfig.script_file) {
                    update.script = {
                        file: this.opConfig.script_file
                    };
                }

                if (this.opConfig.script) {
                    update.script = {
                        source: this.opConfig.script
                    };
                }

                set(update, 'script.params', {});
                for (const [key, field] of Object.entries(this.opConfig.script_params)) {
                    if (record[field]) {
                        // @ts-ignore
                        update.script.params[key] = record[field];
                    }
                }
            } else {
                update.doc = record;
            }

            formatted.push(update);
        } else if (this.opConfig.delete === false) {
            formatted.push(record);
        }
    }

    async onBatch(data: DataEntity[]) {
        const formatted: DataEntity[] = [];

        for (const record of data) {
            this.generateRequest(record, formatted);
        }

        return formatted;
    }
}
