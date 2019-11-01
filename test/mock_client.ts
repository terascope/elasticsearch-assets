
import { AnyObject } from '@terascope/job-components';

export default class MockClient {
    sequence: any[];
    indices: AnyObject;
    cluster: AnyObject;
    deepRecursiveResponseCount: boolean;

    constructor(_sequence?: any[]) {
        const defaultSequence = [
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } }
        ];
        this.sequence = _sequence || defaultSequence;
        this.indices = {};
        this.cluster = {};
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

    async search() {
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
    async bulk(data: any) { return data; }
}
