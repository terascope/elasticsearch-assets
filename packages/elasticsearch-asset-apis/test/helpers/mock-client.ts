import { isPlainObject, isObjectEntity } from '@terascope/utils';

function createData() {
    return {
        _index: 'test-index',
        _type: 'test-type',
        _version: 1,
        _id: 'someId',
        _source: { '@timestamp': new Date() }
    };
}

function validateQuery(obj: Record<string, any>) {
    if (!isPlainObject(obj)) throw new Error('query must be an object');
    if (!obj.index || typeof obj.index !== 'string') throw new Error('query must specify an index');
}

interface BulkData {
    body: Record<string, any>[];
}

function getMeta(meta: Record<string, any> | undefined) {
    if (meta == null) return false;
    if (meta.index) return 'index';
    if (meta.create) return 'create';
    if (meta.update) return 'update';
    if (meta.delete) return 'delete';
    return false;
}

function validateBulk(data: BulkData) {
    if (!data.body) throw new Error('bulk must have a body field');
    if (!Array.isArray(data.body)) throw new Error('bulk data must be an array');

    const areObjects = data.body.every(isObjectEntity);

    if (!areObjects) throw new Error('bulk data must be an array of objects');
    if (data.body.length === 0) throw new Error('bulk must contain an array of objects');

    const clone = data.body.slice();

    while (clone.length > 0) {
        const doc = clone.shift();
        const meta = getMeta(doc);

        if (meta === false) throw new Error('elasticsearch meta data object needs to be paired with data');
        // only delete meta is not paired
        if (meta !== 'delete') {
            const secondDoc = clone.shift();

            if (getMeta(secondDoc)) throw new Error('an elasticsearch meta object must be paired with data if it is not a delete operation');
            if (!isObjectEntity(secondDoc)) throw new Error('data paired with elasticsearch bulk meta must be an object');
            if (Object.keys(secondDoc as Record<string, any>).length === 0) throw new Error('data must not be an empty object');
        }
    }
}

export class MockClient {
    sequence!: Record<string, any>[];
    indices: Record<string, any>;
    cluster: Record<string, any>;
    deepRecursiveResponseCount: boolean | number;
    searchQuery: Record<string, any>;

    constructor(_sequence?: any[], deepRecursiveResponseCount: boolean | number = false) {
        const defaultSequence = [
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } }
        ];

        if (_sequence) {
            this.setSequenceData(_sequence);
        } else {
            this.sequence = defaultSequence;
        }

        this.indices = {};
        this.cluster = {};
        this.searchQuery = {};
        this.deepRecursiveResponseCount = deepRecursiveResponseCount;

        this.indices.getSettings = async () => {
            const window = 10000;
            return {
                some_index: {
                    settings: {
                        index: {
                            max_result_window: window
                        }
                    }
                }
            };
        };

        this.cluster.stats = async () => {
            const defaultVersion = '2.1.1';
            return { nodes: { versions: [defaultVersion] } };
        };
    }

    async search(query: Record<string, any>): Promise<Record<string, any>> {
        validateQuery(query);
        this.searchQuery = query;
        const { sequence } = this;

        if (sequence.length > 0) {
            return sequence.shift() as Record<string, any>;
        }

        const total = this.deepRecursiveResponseCount || 0;
        return {
            _shards: { failed: 0 },
            hits: { total }
        };
    }

    setSequenceData(data: Record<string, any>[]): void {
        this.sequence = data.map(
            (obj: any) => ({
                _shards: { failed: 0 },
                hits: {
                    total: obj.count != null ? obj.count : 100,
                    hits: [{ _source: obj }]
                }
            })
        );
    }

    async bulk(data: BulkData): Promise<BulkData> {
        validateBulk(data);
        return data;
    }
}
