
import { AnyObject, isPlainObject, isSimpleObject } from '@terascope/job-components';

function createData() {
    return {
        _index: 'test-index',
        _type: 'test-type',
        _version: 1,
        _id: 'someId',
        _source: { '@timestamp': new Date() }
    };
}

function validateQuery(obj: AnyObject) {
    if (!isPlainObject(obj)) throw new Error('query must be an object');
    if (!obj.index || typeof obj.index !== 'string') throw new Error('query must specify an index');
}

interface BulkData {
    body: AnyObject[]
}

function validateBulk(data: BulkData) {
    if (!data.body) throw new Error('bulk must have a body field');
    if (!Array.isArray(data.body)) throw new Error('bulk data must be an array');
    const areObjects = data.body.every(isSimpleObject);
    if (!areObjects) throw new Error('bulk data must be an array of objects');
}

export default class MockClient {
    sequence: any[];
    indices: AnyObject;
    cluster: AnyObject;
    deepRecursiveResponseCount: boolean | number;
    searchQuery: AnyObject;

    constructor(_sequence?: any[]) {
        const defaultSequence = [
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [createData()] } }
        ];
        this.sequence = _sequence || defaultSequence;
        this.indices = {};
        this.cluster = {};
        this.searchQuery = {};
        this.deepRecursiveResponseCount = false;

        this.indices.getSettings = async () => {
            const window = 10000;
            return {
                someindex: {
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

    async search(query: AnyObject) {
        validateQuery(query);
        this.searchQuery = query;
        const { sequence } = this;
        if (sequence.length > 0) {
            return sequence.shift();
        }
        const total = this.deepRecursiveResponseCount || 0;
        return {
            _shards: { failed: 0 },
            hits: { total }
        };
    }

    setSequenceData(data: any) {
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

    // eslint-disable-next-line class-methods-use-this
    async bulk(data: BulkData) {
        validateBulk(data);
        return data;
    }
}
